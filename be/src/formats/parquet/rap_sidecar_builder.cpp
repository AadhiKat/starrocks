// See rap_sidecar_builder.h. Format contract: harness/rap_index_build.py (encode) and
// be/src/formats/parquet/rap_index.cpp (parse). Little-endian throughout, except the typed keys
// (RapIndex::encode_int64: sign-flipped big-endian, so bytewise order is numeric order).
#include "formats/parquet/rap_sidecar_builder.h"

#include <algorithm>
#include <cstring>

#include "base/hash/crc32c.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "formats/parquet/rap_index.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks::formats {

namespace {

constexpr char kMagic[4] = {'R', 'A', 'P', 'X'};
constexpr uint32_t kVersion = 2; // slice 4: typed keys + null posting

template <typename T>
void put(std::string* b, T v) {
    b->append(reinterpret_cast<const char*>(&v), sizeof(T));
}

void put_bytes(std::string* b, const std::string& s) {
    put<uint32_t>(b, static_cast<uint32_t>(s.size()));
    b->append(s);
}

// bucket ids -> merged [start, end) runs, adjacent buckets joined (slice 3b)
std::vector<std::pair<int64_t, int64_t>> runs_of(const std::vector<uint32_t>& buckets, uint32_t granularity, uint64_t file_rows) {
    std::vector<std::pair<int64_t, int64_t>> runs;
    for (uint32_t k : buckets) {
        const int64_t start = static_cast<int64_t>(k) * granularity;
        const int64_t end = std::min<int64_t>(static_cast<int64_t>(k + 1) * granularity, static_cast<int64_t>(file_rows));
        if (!runs.empty() && runs.back().second == start) {
            runs.back().second = end;
        } else {
            runs.emplace_back(start, end);
        }
    }
    return runs;
}

void put_runs(std::string* b, const std::vector<std::pair<int64_t, int64_t>>& runs) {
    put<uint32_t>(b, static_cast<uint32_t>(runs.size()));
    for (const auto& [start, end] : runs) {
        put<int64_t>(b, start);
        put<int64_t>(b, end);
    }
}

} // namespace

RapSidecarBuilder::RapSidecarBuilder(std::string column, int32_t field_id, LogicalType type, uint32_t granularity_rows)
        : _column(std::move(column)),
          _field_id(field_id),
          _type(type),
          _granularity(granularity_rows == 0 ? kDefaultGranularityRows : granularity_rows) {}

bool RapSidecarBuilder::supports(LogicalType type) {
    return key_type_of(type) != 0;
}

uint8_t RapSidecarBuilder::key_type_of(LogicalType type) {
    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_CHAR:
        return static_cast<uint8_t>(parquet::RapIndex::KeyType::STRING);
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return static_cast<uint8_t>(parquet::RapIndex::KeyType::INT64);
    case TYPE_BOOLEAN:
        return static_cast<uint8_t>(parquet::RapIndex::KeyType::BOOLEAN);
    case TYPE_DATE:
        return static_cast<uint8_t>(parquet::RapIndex::KeyType::DATE);
    case TYPE_DATETIME:
        return static_cast<uint8_t>(parquet::RapIndex::KeyType::DATETIME);
    default:
        return 0;
    }
}

void RapSidecarBuilder::_add(const std::string& key, uint32_t bucket) {
    if (_over_cap) return;
    auto it = _buckets.find(key);
    if (it == _buckets.end()) {
        // slice 4 (PRD-02): the distinct-value ceiling -- one more key than allowed caps the builder for good
        if (static_cast<int64_t>(_buckets.size()) >= config::rap_index_max_values) {
            _over_cap = true;
            return;
        }
        it = _buckets.emplace(key, std::vector<uint32_t>{}).first;
        _approx_bytes += key.size() + 48;
    }
    auto& v = it->second;
    if (v.empty() || v.back() != bucket) {
        v.push_back(bucket);
        _approx_bytes += sizeof(uint32_t);
    }
}

void RapSidecarBuilder::observe(const Column& written, int64_t first_row) {
    const size_t n = written.size();
    const Column* data = ColumnHelper::get_data_column(&written);
    const NullableColumn* nullable = written.is_nullable() ? down_cast<const NullableColumn*>(&written) : nullptr;
    const uint8_t kt = key_type_of(_type);
    auto bucket_of = [&](size_t i) { return static_cast<uint32_t>((first_row + static_cast<int64_t>(i)) / _granularity); };
    auto add_null = [&](size_t i) {
        const uint32_t b = bucket_of(i);
        if (_null_buckets.empty() || _null_buckets.back() != b) _null_buckets.push_back(b);
    };
    // every branch: NULL rows go to the null posting, values to their canonical key
    switch (static_cast<parquet::RapIndex::KeyType>(kt)) {
    case parquet::RapIndex::KeyType::STRING: {
        const auto* bin = dynamic_cast<const BinaryColumn*>(data);
        if (bin == nullptr) break;
        for (size_t i = 0; i < n; ++i) {
            if (nullable != nullptr && nullable->is_null(i)) {
                add_null(i);
                continue;
            }
            const Slice s = bin->get_slice(i);
            _add(std::string(s.data, s.size), bucket_of(i));
        }
        break;
    }
    case parquet::RapIndex::KeyType::INT64: {
        auto ints = [&](auto getter) {
            for (size_t i = 0; i < n; ++i) {
                if (nullable != nullptr && nullable->is_null(i)) {
                    add_null(i);
                    continue;
                }
                std::string k;
                parquet::RapIndex::encode_int64(getter(i), &k);
                _add(k, bucket_of(i));
            }
        };
        if (const auto* c = dynamic_cast<const Int64Column*>(data)) {
            ints([&](size_t i) { return static_cast<int64_t>(c->get_data()[i]); });
        } else if (const auto* c = dynamic_cast<const Int32Column*>(data)) {
            ints([&](size_t i) { return static_cast<int64_t>(c->get_data()[i]); });
        } else if (const auto* c = dynamic_cast<const Int16Column*>(data)) {
            ints([&](size_t i) { return static_cast<int64_t>(c->get_data()[i]); });
        } else if (const auto* c = dynamic_cast<const Int8Column*>(data)) {
            ints([&](size_t i) { return static_cast<int64_t>(c->get_data()[i]); });
        }
        break;
    }
    case parquet::RapIndex::KeyType::BOOLEAN: {
        const auto* c = dynamic_cast<const BooleanColumn*>(data);
        if (c == nullptr) break;
        for (size_t i = 0; i < n; ++i) {
            if (nullable != nullptr && nullable->is_null(i)) {
                add_null(i);
                continue;
            }
            _add(std::string(1, c->get_data()[i] ? 1 : 0), bucket_of(i));
        }
        break;
    }
    case parquet::RapIndex::KeyType::DATE: {
        const auto* c = dynamic_cast<const DateColumn*>(data);
        if (c == nullptr) break;
        for (size_t i = 0; i < n; ++i) {
            if (nullable != nullptr && nullable->is_null(i)) {
                add_null(i);
                continue;
            }
            std::string k;
            parquet::RapIndex::encode_int64(static_cast<int64_t>(c->get_data()[i].julian()), &k);
            _add(k, bucket_of(i));
        }
        break;
    }
    case parquet::RapIndex::KeyType::DATETIME: {
        const auto* c = dynamic_cast<const TimestampColumn*>(data);
        if (c == nullptr) break;
        for (size_t i = 0; i < n; ++i) {
            if (nullable != nullptr && nullable->is_null(i)) {
                add_null(i);
                continue;
            }
            std::string k;
            parquet::RapIndex::encode_int64(static_cast<int64_t>(c->get_data()[i].timestamp()), &k);
            _add(k, bucket_of(i));
        }
        break;
    }
    default:
        break; // unsupported type: observed as rows only
    }
    _rows_observed += static_cast<int64_t>(n);
}

std::string RapSidecarBuilder::encode(const std::string& file_key, uint64_t file_size, uint64_t file_rows) const {
    std::string b;
    b.append(kMagic, 4);
    put<uint32_t>(&b, kVersion);
    put<uint64_t>(&b, file_size);
    put<uint64_t>(&b, file_rows);
    put_bytes(&b, file_key);
    put_bytes(&b, _column);
    put<int32_t>(&b, _field_id);
    put<uint8_t>(&b, key_type_of(_type));
    put<uint32_t>(&b, _granularity);
    put<uint32_t>(&b, static_cast<uint32_t>(_buckets.size()));
    const auto null_runs = runs_of(_null_buckets, _granularity, file_rows);
    put<uint32_t>(&b, static_cast<uint32_t>(null_runs.size()));
    put<uint64_t>(&b, static_cast<uint64_t>(b.size() + 8)); // postings start right after this field
    // std::map iterates in std::string order == bytewise (char_traits compare is unsigned), the order the reader requires
    for (const auto& [value, buckets] : _buckets) {
        put_bytes(&b, value);
        put_runs(&b, runs_of(buckets, _granularity, file_rows));
    }
    // v2: the null posting's ranges follow the values; their count is the header's n_null_ranges, so no count here
    for (const auto& [start, end] : null_runs) {
        put<int64_t>(&b, start);
        put<int64_t>(&b, end);
    }
    const uint32_t crc = starrocks::crc32c::Value(b.data(), b.size());
    put<uint32_t>(&b, crc);
    b.append(kMagic, 4);
    return b;
}

std::string RapSidecarBuilder::sidecar_path(const std::string& dir, const std::string& file_key) const {
    return dir + (dir.empty() || dir.back() == '/' ? "" : "/") + file_key + "." + _column + kSuffix;
}

} // namespace starrocks::formats
