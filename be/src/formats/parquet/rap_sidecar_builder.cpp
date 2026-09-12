// See rap_sidecar_builder.h. Format contract: harness/rap_index_build.py (encode) and
// be/src/formats/parquet/rap_index.cpp (parse). Little-endian throughout.
#include "formats/parquet/rap_sidecar_builder.h"

#include <algorithm>
#include <cstring>

#include "base/hash/crc32c.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"

namespace starrocks::formats {

namespace {

constexpr char kMagic[4] = {'R', 'A', 'P', 'X'};
constexpr uint32_t kVersion = 1;

template <typename T>
void put(std::string* b, T v) {
    b->append(reinterpret_cast<const char*>(&v), sizeof(T));
}

void put_bytes(std::string* b, const std::string& s) {
    put<uint32_t>(b, static_cast<uint32_t>(s.size()));
    b->append(s);
}

} // namespace

RapSidecarBuilder::RapSidecarBuilder(std::string column, int32_t field_id, uint32_t granularity_rows)
        : _column(std::move(column)), _field_id(field_id), _granularity(granularity_rows == 0 ? kDefaultGranularityRows : granularity_rows) {}

void RapSidecarBuilder::observe(const Column& written, int64_t first_row) {
    const size_t n = written.size();
    const Column* data = ColumnHelper::get_data_column(&written);
    const auto* bin = dynamic_cast<const BinaryColumn*>(data);
    if (bin == nullptr) {
        // only string columns are indexed; anything else is observed as rows only
        _rows_observed += static_cast<int64_t>(n);
        return;
    }
    const NullableColumn* nullable = written.is_nullable() ? down_cast<const NullableColumn*>(&written) : nullptr;
    for (size_t i = 0; i < n; ++i) {
        if (nullable != nullptr && nullable->is_null(i)) continue;
        const Slice s = bin->get_slice(i);
        const uint32_t bucket = static_cast<uint32_t>((first_row + static_cast<int64_t>(i)) / _granularity);
        auto& v = _buckets[std::string(s.data, s.size)];
        if (v.empty() || v.back() != bucket) v.push_back(bucket);
    }
    _rows_observed += static_cast<int64_t>(n);
}

std::string RapSidecarBuilder::encode(const std::string& file_basename, uint64_t file_size, uint64_t file_rows) const {
    std::string b;
    b.append(kMagic, 4);
    put<uint32_t>(&b, kVersion);
    put<uint64_t>(&b, file_size);
    put<uint64_t>(&b, file_rows);
    put_bytes(&b, file_basename);
    put_bytes(&b, _column);
    put<int32_t>(&b, _field_id);
    put<uint32_t>(&b, _granularity);
    put<uint32_t>(&b, static_cast<uint32_t>(_buckets.size()));
    put<uint64_t>(&b, static_cast<uint64_t>(b.size() + 8)); // postings start right after this field
    // std::map iterates in std::string order == bytewise (char_traits compare is unsigned), the order the reader requires
    for (const auto& [value, buckets] : _buckets) {
        put_bytes(&b, value);
        // slice 3b: adjacent buckets (k, k+1, ...) become ONE range -- the buckets are ascending and deduplicated
        // by observe(), so a run is closed when the next bucket is not the previous plus one. The reader merges at
        // lookup anyway; this only removes the redundant bytes (D6: ~5x the offline sidecar for the same rows).
        std::vector<std::pair<int64_t, int64_t>> runs;
        for (uint32_t k : buckets) {
            const int64_t start = static_cast<int64_t>(k) * _granularity;
            const int64_t end = std::min<int64_t>(static_cast<int64_t>(k + 1) * _granularity, static_cast<int64_t>(file_rows));
            if (!runs.empty() && runs.back().second == start) {
                runs.back().second = end;
            } else {
                runs.emplace_back(start, end);
            }
        }
        put<uint32_t>(&b, static_cast<uint32_t>(runs.size()));
        for (const auto& [start, end] : runs) {
            put<int64_t>(&b, start);
            put<int64_t>(&b, end);
        }
    }
    const uint32_t crc = starrocks::crc32c::Value(b.data(), b.size());
    put<uint32_t>(&b, crc);
    b.append(kMagic, 4);
    return b;
}

std::string RapSidecarBuilder::sidecar_path(const std::string& dir, const std::string& file_basename) const {
    return dir + (dir.empty() || dir.back() == '/' ? "" : "/") + file_basename + "." + _column + kSuffix;
}

} // namespace starrocks::formats
