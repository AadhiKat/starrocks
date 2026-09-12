// RAP / lake-index slice milestone 1: a persistent per-file sidecar index (format RAPX) for
// predicates on one column, consulted by the Parquet reader before any page is read, feeding the
// existing row-range transport (FormatScanContext::selected_row_ranges -> GroupReader::_range).
//
// Readiness is a GATE, not a hope: the sidecar is refused -- and the scan proceeds unindexed and
// complete -- on bad magic at either end, unknown version, crc32c mismatch, file identity mismatch
// (key, size, rows against the Parquet footer), column name / field-id mismatch against the
// schema, unsorted values, or (slice 4, PRD-02) a declared count the body cannot hold or a sidecar
// above the size ceiling. Identity is what StarRocks already uses for its file caches (name + size;
// mtime is not available to the builder and lake files are immutable).
//
// Slice 2c: the sidecar is read through a FileSystem handed in by the scan (the same connector that
// opened the data file), so a gs:// sidecar directory works like a local one. A null filesystem
// means FileSystem::Default().
//
// Slice 4: RAPX v2 adds a key TYPE (canonical byte encodings whose bytewise order is the type's order,
// so ranges can be answered) and a NULL posting; v1 (string keys, no null posting) stays readable.
// Every answer is a CANDIDATE set of row ranges: the predicate is still evaluated on the rows read.
//
// The format is defined once, in harness/rap_index_build.py (builder + reference decoder); this
// file implements the same contract. Little-endian throughout, except the typed keys (big-endian by
// construction so that bytewise order is numeric order).
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "formats/scan_context.h" // RowRangeHint
#include "types/logical_type.h"

namespace starrocks {
class FileSystem;
class Datum;
} // namespace starrocks

namespace starrocks::parquet {

class RapIndex {
public:
    enum class State { ABSENT, UNUSABLE, READY };

    // slice 4: the type of the keys a sidecar carries. Canonical encodings: STRING = the bytes as written;
    // INT64 = the value (TINYINT..BIGINT widened) with its sign bit flipped, big-endian, 8 bytes; BOOLEAN = one byte
    // 0 / 1; DATE = the julian day as INT64; DATETIME = the internal timestamp (monotonic in time) as INT64.
    enum class KeyType : uint8_t { STRING = 1, INT64 = 2, BOOLEAN = 3, DATE = 4, DATETIME = 5 };

    struct Identity {
        std::string file_name; // KEY of the data file (key_of): its full path minus the scheme (slice 2g v3)
        uint64_t file_size = 0;
        uint64_t file_rows = 0;
        std::string column;
        int32_t field_id = -1; // -1 when the schema carries no field ids
    };

    struct Result {
        State state = State::ABSENT;
        std::string reason; // populated for UNUSABLE, and for ABSENT with the step that reported absence (slice 2e)
        std::unique_ptr<RapIndex> index; // populated for READY
    };

    static constexpr const char* kSuffix = ".rapx";
    // slice 2f: what the cache remembers about a refused consult (per generation, directory, file identity, column)
    struct NegativeEntry {
        bool unusable = false; // false = ABSENT
        std::string reason;
    };
    // slice 2f v2 (astra CX-45): the ONE place both cache keys are built. READY and negative entries hold different C++
    // types, so their keys must never coincide: a namespace tag per kind and a length prefix on every variable field
    // make the mapping (kind, file_key, column, generation, directory) -> key injective, whatever the strings contain.
    // The first form concatenated raw strings, and the generation "g1|neg|<dir>" aliased a negative entry as READY.
    static std::string cache_key(bool negative, const std::string& file_key, const std::string& column,
                                 const std::string& generation, const std::string& directory);
    static constexpr uint32_t kVersion = 1;   // the version the v1 tests and the reference encoder emit
    static constexpr uint32_t kVersionV2 = 2; // slice 4: typed keys + NULL posting
    // slice 2g v3 (fork production-readiness review, PRD-01): the KEY of a data file is its FULL path minus its scheme
    // and leading slashes -- bucket, table location, partition directories and file name, nothing shortened. v2's "path
    // after the last /data/" dropped the table, so two tables with the same suffix, size and row count under one sidecar
    // directory shared a sidecar; the full path is unambiguous across tables and buckets. It names the sidecar object
    // (<dir>/<key>.<column>.rapx), is the identity stored in the sidecar, and is the FE manifest's file name
    // (RapCoverage.keyOf applies the same rule).
    static std::string key_of(const std::string& path);

    // Load `path` through `fs` (null = FileSystem::Default()). Never throws; a missing file is ABSENT,
    // any other open / read error and anything that is not a valid, identity-matching sidecar is
    // UNUSABLE with a reason.
    static Result load(FileSystem* fs, const std::string& path, const Identity& expect);
    // Same, through the default filesystem.
    static Result load(const std::string& path, const Identity& expect);
    // Same gate over in-memory bytes (tests, and later a cache).
    static Result parse(const std::string& bytes, const Identity& expect);

    // slice 4: canonical key bytes for a predicate literal of logical type `lt` against a sidecar of key type `kt`.
    // false when the literal's type cannot be encoded as that key type (the consult is then UNUSABLE: literal type).
    static bool encode_literal(KeyType kt, LogicalType lt, const Datum& d, std::string* out);
    static void encode_int64(int64_t v, std::string* out);

    // Equality / IN: the union of the ranges of every listed value (canonical key bytes), merged into ascending,
    // non-overlapping, half-open [start, end) row intervals. A value absent from the index contributes nothing; an
    // empty result means "no row of this file can match".
    std::vector<RowRangeHint> lookup(const std::vector<std::string>& values) const;
    // slice 4: every key in the ordered interval (null pointer = unbounded on that side), inclusive flags as SQL.
    std::vector<RowRangeHint> lookup_range(const std::string* lower, bool lower_inclusive, const std::string* upper,
                                           bool upper_inclusive) const;
    // slice 4: the NULL posting (v2), and the conservative complement (every value key's ranges).
    std::vector<RowRangeHint> null_ranges() const;
    std::vector<RowRangeHint> not_null_ranges() const;

    // Intersection of two ascending non-overlapping range lists (compose with a transport hint).
    static std::vector<RowRangeHint> intersect(const std::vector<RowRangeHint>& a, const std::vector<RowRangeHint>& b);
    // Sort + merge into ascending non-overlapping ranges.
    static std::vector<RowRangeHint> merge(std::vector<RowRangeHint> all);

    uint32_t version() const { return _version; }
    KeyType key_type() const { return _key_type; }
    uint32_t granularity_rows() const { return _granularity_rows; }
    size_t num_values() const { return _values.size(); }
    // slice 3b: the number of ranges as ENCODED (before lookup's merge) -- what the sidecar's bytes carry
    size_t num_ranges() const {
        size_t n = 0;
        for (const auto& rs : _ranges) n += rs.size();
        return n;
    }
    const Identity& identity() const { return _identity; }
    // rough resident size, for the cache's accounting
    size_t approx_bytes() const {
        size_t n = 0;
        for (size_t i = 0; i < _values.size(); ++i) n += _values[i].size() + 32 + _ranges[i].size() * sizeof(RowRangeHint);
        return n + _null_ranges.size() * sizeof(RowRangeHint);
    }

private:
    Identity _identity;
    uint32_t _version = kVersion;
    KeyType _key_type = KeyType::STRING;
    uint32_t _granularity_rows = 0;
    std::vector<std::string> _values;                 // sorted bytewise ascending (= the key type's order, slice 4)
    std::vector<std::vector<RowRangeHint>> _ranges;   // parallel to _values
    std::vector<RowRangeHint> _null_ranges;           // v2: rows whose value is NULL (candidate buckets)
};

} // namespace starrocks::parquet
