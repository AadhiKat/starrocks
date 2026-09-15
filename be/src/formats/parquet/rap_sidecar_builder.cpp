// See rap_sidecar_builder.h. Format contract: harness/rap_index_build.py (encode) and
// be/src/formats/parquet/rap_index.cpp (parse). Little-endian throughout, except the typed keys
// (RapIndex::encode_int64: sign-flipped big-endian, so bytewise order is numeric order).
#include "formats/parquet/rap_sidecar_builder.h"

#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>

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
constexpr uint32_t kVersion = 5; // RAPX v5: granule-native postings, or a per-granule min/max zone map

template <typename T>
void put(std::string* b, T v) {
    b->append(reinterpret_cast<const char*>(&v), sizeof(T));
}

void put_bytes(std::string* b, const std::string& s) {
    put<uint32_t>(b, static_cast<uint32_t>(s.size()));
    b->append(s);
}

// ---- v5 primitives. Every one has a line-for-line twin in harness/rap_index_build.py.
void put_uvarint(std::string* b, uint64_t n) {
    while (true) {
        const uint8_t x = static_cast<uint8_t>(n & 0x7f);
        n >>= 7;
        b->push_back(static_cast<char>(n != 0 ? (x | 0x80) : x));
        if (n == 0) return;
    }
}

size_t uvarint_len(uint64_t n) {
    size_t k = 1;
    while (n >= 128) {
        n >>= 7;
        ++k;
    }
    return k;
}

size_t shared_prefix(const std::string& a, const std::string& b) {
    const size_t m = std::min(a.size(), b.size());
    size_t i = 0;
    while (i < m && a[i] == b[i]) ++i;
    return i;
}

// granule ordinals -> [start, end) runs over ORDINALS (not rows), adjacent ones joined
std::vector<std::pair<uint32_t, uint32_t>> granule_runs(const std::vector<uint32_t>& buckets) {
    std::vector<std::pair<uint32_t, uint32_t>> runs;
    for (uint32_t g : buckets) {
        if (!runs.empty() && runs.back().second == g) {
            runs.back().second = g + 1;
        } else {
            runs.emplace_back(g, g + 1);
        }
    }
    return runs;
}

constexpr size_t kRangeBytes = 16; // one (i64 start, i64 end) pair, as the null posting writes it

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

RapSidecarBuilder::Choice RapSidecarBuilder::shape_of(uint64_t file_size, uint64_t file_rows) const {
    Choice ch;
    ch.n_granules = _granularity == 0 ? 0 : static_cast<uint32_t>((file_rows + _granularity - 1) / _granularity);
    const size_t width = (static_cast<size_t>(ch.n_granules) + 7) / 8;
    // the null posting rides with the postings shape, and costs the same under either encoding
    const size_t null_bytes = kRangeBytes * runs_of(_null_buckets, _granularity, file_rows).size();

    size_t keys = 0, bitmap = 0, runs_b = 0;
    std::string prev;
    for (const auto& [value, buckets] : _buckets) {
        const size_t sh = shared_prefix(prev, value);
        keys += uvarint_len(sh) + uvarint_len(value.size() - sh) + (value.size() - sh);
        prev = value;
        bitmap += width;
        const auto runs = granule_runs(buckets);
        runs_b += uvarint_len(runs.size());
        uint32_t p = 0;
        for (const auto& [a, b] : runs) {
            runs_b += uvarint_len(a - p) + uvarint_len(b - a - 1);
            p = b;
        }
    }
    if (bitmap <= runs_b) {
        ch.encoding = parquet::RapIndex::PostingEncoding::BITMAP;
        ch.postings_bytes = keys + bitmap + null_bytes;
    } else {
        ch.encoding = parquet::RapIndex::PostingEncoding::RUNS;
        ch.postings_bytes = keys + runs_b + null_bytes;
    }

    // the zone map's size depends only on the granule count and the key width -- it is never the expensive one
    const uint8_t kt = key_type_of(_type);
    if (kt == static_cast<uint8_t>(parquet::RapIndex::KeyType::STRING)) {
        std::vector<const std::string*> zmin(ch.n_granules, nullptr), zmax(ch.n_granules, nullptr);
        for (const auto& [value, buckets] : _buckets) {
            for (uint32_t g : buckets) {
                if (g >= ch.n_granules) continue;
                if (zmin[g] == nullptr) zmin[g] = &value;
                zmax[g] = &value;
            }
        }
        for (uint32_t g = 0; g < ch.n_granules; ++g) {
            const size_t lo = zmin[g] == nullptr ? 0 : zmin[g]->size();
            const size_t hi = zmax[g] == nullptr ? 0 : zmax[g]->size();
            ch.zonemap_bytes += 1 + uvarint_len(lo) + lo + uvarint_len(hi) + hi;
        }
    } else {
        const size_t w = kt == static_cast<uint8_t>(parquet::RapIndex::KeyType::BOOLEAN) ? 1 : 8;
        ch.zonemap_bytes = static_cast<size_t>(ch.n_granules) * (1 + 2 * w);
    }

    // THE RULE. Postings while they fit the per-file share of the data file that S8 allows; otherwise the zone map.
    // A file whose size is not known (0 -- synthetic callers only; both real build paths have it at close) falls back
    // to the structural form the same decision rests on: distinct values above rows / 64 means postings approach one
    // entry per row.
    if (file_size > 0) {
        const double budget = config::rap_index_postings_budget_pct * static_cast<double>(file_size) / 100.0;
        ch.shape = static_cast<double>(ch.postings_bytes) > budget ? parquet::RapIndex::Shape::ZONEMAP
                                                                   : parquet::RapIndex::Shape::POSTINGS;
    } else {
        ch.shape = _buckets.size() * 64 > std::max<uint64_t>(file_rows, 1) ? parquet::RapIndex::Shape::ZONEMAP
                                                                           : parquet::RapIndex::Shape::POSTINGS;
    }
    if (ch.shape == parquet::RapIndex::Shape::ZONEMAP) ch.encoding = parquet::RapIndex::PostingEncoding::NONE;
    return ch;
}

std::string RapSidecarBuilder::encode(const std::string& file_key, uint64_t file_size, uint64_t file_rows,
                                      const Choice* force) const {
    const Choice ch = force != nullptr ? *force : shape_of(file_size, file_rows);
    const bool zonemap = ch.shape == parquet::RapIndex::Shape::ZONEMAP;
    const auto null_runs = runs_of(_null_buckets, _granularity, file_rows);

    std::string b;
    b.append(kMagic, 4);
    put<uint32_t>(&b, kVersion);
    put<uint64_t>(&b, file_size);
    put<uint64_t>(&b, file_rows);
    put_bytes(&b, file_key);
    put_bytes(&b, _column);
    put<int32_t>(&b, _field_id);
    put<uint8_t>(&b, key_type_of(_type));
    put<uint8_t>(&b, static_cast<uint8_t>(ch.shape));
    put<uint8_t>(&b, static_cast<uint8_t>(zonemap ? parquet::RapIndex::PostingEncoding::NONE : ch.encoding));
    put<uint32_t>(&b, _granularity);
    put<uint32_t>(&b, ch.n_granules);
    // the distinct non-NULL values measured in this file: the selection rule's numerator, kept so the choice can be
    // audited from the sidecar alone -- a zone map otherwise says nothing about why it is one
    put<uint64_t>(&b, static_cast<uint64_t>(_buckets.size()));
    put<uint32_t>(&b, static_cast<uint32_t>(zonemap ? 0 : _buckets.size()));
    put<uint32_t>(&b, static_cast<uint32_t>(zonemap ? 0 : null_runs.size()));
    put<uint64_t>(&b, static_cast<uint64_t>(b.size() + 8)); // the body starts right after this field

    if (!zonemap) {
        // std::map iterates in std::string order == bytewise (char_traits compare is unsigned), the order the reader
        // requires and the order front coding needs
        const size_t width = (static_cast<size_t>(ch.n_granules) + 7) / 8;
        const bool bitmap = ch.encoding == parquet::RapIndex::PostingEncoding::BITMAP;
        std::string prev;
        for (const auto& [value, buckets] : _buckets) {
            const size_t sh = shared_prefix(prev, value);
            put_uvarint(&b, sh);
            put_uvarint(&b, value.size() - sh);
            b.append(value, sh, value.size() - sh);
            prev = value;
            if (bitmap) {
                const size_t at = b.size();
                b.append(width, '\0');
                for (uint32_t g : buckets) {
                    if (g < ch.n_granules) b[at + (g >> 3)] = static_cast<char>(b[at + (g >> 3)] | (1u << (g & 7u)));
                }
            } else {
                const auto runs = granule_runs(buckets);
                put_uvarint(&b, runs.size());
                uint32_t p = 0;
                for (const auto& [a, e] : runs) {
                    put_uvarint(&b, a - p);
                    put_uvarint(&b, e - a - 1);
                    p = e;
                }
            }
        }
        // the null posting's ranges follow the values; their count is the header's n_null_ranges, so no count here
        for (const auto& [start, end] : null_runs) {
            put<int64_t>(&b, start);
            put<int64_t>(&b, end);
        }
    } else {
        // the zone map from the very map the builder already holds: ascending iteration means the first key to touch
        // a granule is its minimum and the last is its maximum
        std::vector<const std::string*> zmin(ch.n_granules, nullptr), zmax(ch.n_granules, nullptr);
        for (const auto& [value, buckets] : _buckets) {
            for (uint32_t g : buckets) {
                if (g >= ch.n_granules) continue;
                if (zmin[g] == nullptr) zmin[g] = &value;
                zmax[g] = &value;
            }
        }
        std::vector<uint8_t> has_null(ch.n_granules, 0);
        for (uint32_t g : _null_buckets) {
            if (g < ch.n_granules) has_null[g] = 1;
        }
        const uint8_t kt = key_type_of(_type);
        const bool var = kt == static_cast<uint8_t>(parquet::RapIndex::KeyType::STRING);
        const size_t w = kt == static_cast<uint8_t>(parquet::RapIndex::KeyType::BOOLEAN) ? 1 : 8;
        for (uint32_t g = 0; g < ch.n_granules; ++g) {
            // no value key touches this granule -> it holds no value at all. Every granule has at least one row, so
            // that is exactly the all-NULL case, and the flag is what keeps its placeholder bounds out of every
            // equality and range answer.
            uint8_t flags = has_null[g] != 0 ? parquet::RapIndex::kZoneHasNull : 0;
            if (zmin[g] == nullptr) flags |= parquet::RapIndex::kZoneAllNull;
            put<uint8_t>(&b, flags);
            if (var) {
                const std::string empty;
                const std::string& lo = zmin[g] == nullptr ? empty : *zmin[g];
                const std::string& hi = zmax[g] == nullptr ? empty : *zmax[g];
                put_uvarint(&b, lo.size());
                b.append(lo);
                put_uvarint(&b, hi.size());
                b.append(hi);
            } else if (zmin[g] == nullptr) {
                b.append(2 * w, '\0');
            } else {
                b.append(*zmin[g]);
                b.append(*zmax[g]);
            }
        }
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
