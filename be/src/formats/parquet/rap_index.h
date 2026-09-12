// RAP / lake-index slice milestone 1: a persistent per-file sidecar index (format RAPX v1) for
// equality / IN on one column, consulted by the Parquet reader before any page is read, feeding the
// existing row-range transport (FormatScanContext::selected_row_ranges -> GroupReader::_range).
//
// Readiness is a GATE, not a hope: the sidecar is refused -- and the scan proceeds unindexed and
// complete -- on bad magic at either end, unknown version, crc32c mismatch, file identity mismatch
// (basename, size, rows against the Parquet footer), column name / field-id mismatch against the
// schema, or unsorted values. Identity is what StarRocks already uses for its file caches
// (name + size; mtime is not available to the builder and lake files are immutable).
//
// Slice 2c: the sidecar is read through a FileSystem handed in by the scan (the same connector that
// opened the data file), so a gs:// sidecar directory works like a local one. A null filesystem
// means FileSystem::Default().
//
// The format is defined once, in harness/rap_index_build.py (builder + reference decoder); this
// file implements the same contract. Little-endian throughout.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "formats/scan_context.h" // RowRangeHint

namespace starrocks {
class FileSystem;
}

namespace starrocks::parquet {

class RapIndex {
public:
    enum class State { ABSENT, UNUSABLE, READY };

    struct Identity {
        std::string file_name; // KEY of the data file (key_of): its path under the table's data/ root, or the basename
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
    static constexpr uint32_t kVersion = 1;
    // slice 2g (F-COLLISION): the KEY of a data file is its path relative to the table's data/ root -- the part after
    // the LAST "/data/" segment; a path without one keeps the basename. The sink names files per partition
    // directory, so a basename is not unique on a partitioned table; the key is. It names the sidecar object
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

    // Equality / IN: the union of the ranges of every listed value, merged into ascending,
    // non-overlapping, half-open [start, end) row intervals. A value absent from the index
    // contributes nothing; an empty result means "no row of this file can match".
    std::vector<RowRangeHint> lookup(const std::vector<std::string>& values) const;

    // Intersection of two ascending non-overlapping range lists (compose with a transport hint).
    static std::vector<RowRangeHint> intersect(const std::vector<RowRangeHint>& a, const std::vector<RowRangeHint>& b);

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
        return n;
    }

private:
    Identity _identity;
    uint32_t _granularity_rows = 0;
    std::vector<std::string> _values;                 // sorted bytewise ascending
    std::vector<std::vector<RowRangeHint>> _ranges;   // parallel to _values
};

} // namespace starrocks::parquet
