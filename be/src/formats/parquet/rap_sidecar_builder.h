// RAP / lake-index slice 3a: index at export. While the Iceberg sink writes a Parquet file, this builder
// observes the WRITTEN values of one string column (post-evaluator), buckets row positions by
// `granularity_rows`, and at close encodes a RAPX v1 sidecar (harness/rap_index_build.py defines the
// format; be/src/formats/parquet/rap_index.cpp is the consumer) carrying the identity the reader checks:
// basename, final file size, row count, column, field id.
//
// Nulls are not indexed (EQ / IN never match NULL). Values are kept as raw bytes and sorted bytewise on
// encode, which is the order the reader requires.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"

namespace starrocks::formats {

class RapSidecarBuilder {
public:
    static constexpr uint32_t kDefaultGranularityRows = 20000;
    static constexpr const char* kSuffix = ".rapx";

    RapSidecarBuilder(std::string column, int32_t field_id, uint32_t granularity_rows = kDefaultGranularityRows);

    // Observe the written column of one chunk; `first_row` is the absolute row position of its first row.
    void observe(const Column& written, int64_t first_row);

    // Encode the finished file's sidecar bytes (RAPX v1). `file_rows` bounds the last bucket.
    std::string encode(const std::string& file_basename, uint64_t file_size, uint64_t file_rows) const;

    // <dir>/<basename>.<column>.rapx -- the per-column name slice 2c's consult tries first
    std::string sidecar_path(const std::string& dir, const std::string& file_basename) const;

    const std::string& column() const { return _column; }
    int32_t field_id() const { return _field_id; }
    uint32_t granularity_rows() const { return _granularity; }
    size_t num_values() const { return _buckets.size(); }
    int64_t rows_observed() const { return _rows_observed; }

private:
    std::string _column;
    int32_t _field_id;
    uint32_t _granularity;
    int64_t _rows_observed = 0;
    // value bytes -> ascending, de-duplicated bucket ids (rows arrive in ascending order)
    std::map<std::string, std::vector<uint32_t>> _buckets;
};

} // namespace starrocks::formats
