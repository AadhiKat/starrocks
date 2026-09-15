// RAP / lake-index slice 3a: index at export. While the Iceberg sink writes a Parquet file, this builder
// observes the WRITTEN values of one column (post-evaluator), buckets row positions by
// `granularity_rows`, and at close encodes a RAPX sidecar (harness/rap_index_build.py defines the
// format; be/src/formats/parquet/rap_index.cpp is the consumer) carrying the identity the reader checks:
// key (slice 2g v3: the file's full path minus its scheme), final file size, row count, column, field id.
//
// Slice 4: typed keys (RAPX v2). A column of type VARCHAR / CHAR, TINYINT .. BIGINT, BOOLEAN, DATE or DATETIME is
// observed through its canonical key encoding (RapIndex::KeyType), so the reader can answer ranges; NULL rows are
// indexed as a separate null posting. Slice 4 also feeds this builder from the scan side (P3b). A distinct-value
// ceiling (config::rap_index_max_values, PRD-02) stops the builder; a capped builder writes nothing.
//
// Keys are kept as canonical bytes and sorted bytewise on encode, which is the order the reader requires and, for the
// typed encodings, the type's own order.
//
// RAPX v5 (2026-09-15, acceptance row S8): `encode` now writes the cardinality-aware format, and it is the ONE place
// both build paths go through -- the export sink (ParquetFileWriter::_write_rap_sidecars) and the scan-and-build path
// (FileReader) share this object, so they cannot diverge. At encode time the builder already holds everything the
// decision needs, so it is made on EXACT byte counts rather than an estimate:
//
//   1. the granule sets it accumulated ARE v5's postings -- `_buckets` is already key -> ascending granule ordinals --
//      so both posting encodings (bitmap, delta-varint runs) are measured and the smaller is written;
//   2. the per-granule min/max ZONE MAP falls out of the same map for free: std::map iterates bytewise ascending, so
//      the first key to touch a granule is its minimum and the last is its maximum. No extra state during the write.
//   3. POSTINGS are written while their exact body is within config::rap_index_postings_budget_pct % of the data
//      file's bytes; otherwise the ZONE MAP. A near-unique column therefore costs ~0.014 % of the file instead of
//      200 %, and a dimension keeps the postings that narrow.
//
// `shape_of` exposes the decision (and the byte counts behind it) so a test can assert the rule rather than the
// outcome, and `encode` takes an optional forced shape so both shapes can be exercised on the same data.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "formats/parquet/rap_index.h"
#include "types/logical_type.h"

namespace starrocks::formats {

class RapSidecarBuilder {
public:
    static constexpr uint32_t kDefaultGranularityRows = 20000;
    static constexpr const char* kSuffix = ".rapx";

    // `type` is the column's logical type; unsupported types are observed as rows only (see supports())
    RapSidecarBuilder(std::string column, int32_t field_id, LogicalType type = TYPE_VARCHAR,
                      uint32_t granularity_rows = kDefaultGranularityRows);

    // slice 4: the types a sidecar can be built for
    static bool supports(LogicalType type);
    // the RAPX v2 key type byte for a supported logical type (0 when unsupported)
    static uint8_t key_type_of(LogicalType type);

    // Observe the written column of one chunk; `first_row` is the absolute row position of its first row.
    void observe(const Column& written, int64_t first_row);

    // v5: what the selection rule decides for this file, and the byte counts it decided on.
    struct Choice {
        parquet::RapIndex::Shape shape = parquet::RapIndex::Shape::POSTINGS;
        parquet::RapIndex::PostingEncoding encoding = parquet::RapIndex::PostingEncoding::BITMAP;
        size_t postings_bytes = 0; // exact body bytes under `encoding`, null posting included
        size_t zonemap_bytes = 0;  // exact body bytes of the zone map
        uint32_t n_granules = 0;
    };
    Choice shape_of(uint64_t file_size, uint64_t file_rows) const;

    // Encode the finished file's sidecar bytes (RAPX v5). `file_rows` bounds the last granule. `file_key` is the
    // file's key (RapIndex::key_of). With `force` null the selection rule chooses the shape; pass one to write a
    // named shape on the same data (tests).
    std::string encode(const std::string& file_key, uint64_t file_size, uint64_t file_rows,
                       const Choice* force = nullptr) const;

    // <dir>/<key>.<column>.rapx -- the per-column name slice 2c's consult tries first
    std::string sidecar_path(const std::string& dir, const std::string& file_key) const;

    const std::string& column() const { return _column; }
    int32_t field_id() const { return _field_id; }
    LogicalType type() const { return _type; }
    uint32_t granularity_rows() const { return _granularity; }
    size_t num_values() const { return _buckets.size(); }
    size_t num_null_buckets() const { return _null_buckets.size(); }
    int64_t rows_observed() const { return _rows_observed; }
    // slice 4 (PRD-02): the distinct-value ceiling was hit; the sidecar must not be written
    bool over_cap() const { return _over_cap; }
    size_t approx_bytes() const { return _approx_bytes; }

private:
    void _add(const std::string& key, uint32_t bucket);

    std::string _column;
    int32_t _field_id;
    LogicalType _type;
    uint32_t _granularity;
    int64_t _rows_observed = 0;
    bool _over_cap = false;
    size_t _approx_bytes = 0;
    // canonical key bytes -> ascending, de-duplicated bucket ids (rows arrive in ascending order)
    std::map<std::string, std::vector<uint32_t>> _buckets;
    std::vector<uint32_t> _null_buckets; // v2: buckets holding at least one NULL
};

} // namespace starrocks::formats
