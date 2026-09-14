// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "formats/parquet/rap_index_cache.h"

#include <chrono>

#include "formats/parquet/utils.h"

namespace starrocks::parquet {

std::string RapIndexCache::key(const std::string& uri, uint64_t size, int64_t mtime,
                              const std::string& column, const std::string& generation) {
    return RapIndex::cache_key(false, ParquetUtils::get_file_cache_key(CacheType::INDEX, uri, mtime, size),
                               column, generation, "");
}

bool RapIndexCache::compatible(const RapIndex& index, const RapIndex::Identity& expected) {
    const auto& id = index.identity();
    return id.file_name == expected.file_name && id.file_size == expected.file_size &&
           id.file_rows == expected.file_rows && id.column == expected.column &&
           (id.field_id < 0 || expected.field_id < 0 || id.field_id == expected.field_id);
}

std::shared_ptr<RapIndex> RapIndexCache::lookup(StoragePageCache* cache, const std::string& key,
                                               const RapIndex::Identity& expected) {
    PageCacheHandle handle;
    if (cache == nullptr || !cache->available() || !cache->lookup(key, &handle)) return nullptr;
    auto index = *reinterpret_cast<const std::shared_ptr<RapIndex>*>(handle.data());
    return index && compatible(*index, expected) ? index : nullptr;
}

Status RapIndexCache::insert(StoragePageCache* cache, const std::string& key, std::shared_ptr<RapIndex> index,
                            MemCacheWriteOptions options) {
    if (cache == nullptr || !cache->available()) return Status::NotSupported("RAP parsed cache unavailable");
    const int64_t charge = static_cast<int64_t>(index->approx_bytes());
    auto capture = std::make_unique<std::shared_ptr<RapIndex>>(std::move(index));
    auto deleter = [](const starrocks::CacheKey&, void* value) {
        delete static_cast<std::shared_ptr<RapIndex>*>(value);
    };
    PageCacheHandle handle;
    auto status = cache->insert(key, capture.get(), charge, deleter, options, &handle);
    if (status.ok()) capture.release();
    return status;
}

bool RapIndexCache::resident(StoragePageCache* cache, const RapIndexPreloader::Request& r) {
    return lookup(cache, key(r.file_uri, r.file_size, r.modification_time, r.column, r.generation),
                  {RapIndex::key_of(r.file_uri), r.file_size, r.file_rows, r.column, r.field_id}) != nullptr;
}

RapIndexPreloader::Prepared RapIndexCache::prepare(StoragePageCache* cache, FileSystem* fs,
                                                   const RapIndexPreloader::Request& r) {
    using Result = RapIndexPreloader::Result;
    if (cache == nullptr || !cache->available()) return {Result{"NO_CACHE"}, {}};
    if (resident(cache, r)) return {Result{"CACHED"}, {}};
    RapIndex::Identity expected{RapIndex::key_of(r.file_uri), r.file_size, r.file_rows, r.column, r.field_id};
    RapIndex::LoadStats stats;
    // New preload requests use the canonical per-column name only. Legacy demand
    // fallback stays unchanged; no doubling of speculative absence probes.
    auto loaded = RapIndex::load(fs, r.directory + "/" + expected.file_name + "." + r.column + RapIndex::kSuffix,
                                 expected, &stats, static_cast<int64_t>(r.max_bytes));
    Result result;
    result.bytes = stats.bytes;
    result.attempts = stats.attempts;
    result.exists_ns = stats.exists_ns;
    result.open_ns = stats.open_ns;
    result.size_ns = stats.size_ns;
    result.read_ns = stats.read_ns;
    result.parse_ns = stats.parse_ns;
    if (loaded.state != RapIndex::State::READY) {
        result.state = loaded.state == RapIndex::State::ABSENT ? "ABSENT" : "UNUSABLE";
        return {result, {}}; // Speculative failures do not populate the demand negative cache.
    }
    auto index = std::shared_ptr<RapIndex>(std::move(loaded.index));
    const auto cache_key = key(r.file_uri, r.file_size, r.modification_time, r.column, r.generation);
    return {result, [cache, cache_key, index = std::move(index), expected, result]() mutable {
        const auto start = std::chrono::steady_clock::now();
        if (lookup(cache, cache_key, expected)) {
            result.state = "CACHED";
        } else {
            // Same bounded LRU as demand reads, not a pin. In this cache engine
            // evict_probability=0 rejects ALL writes, even when space is free.
            result.state = insert(cache, cache_key, index).ok() ? "LOADED" : "NO_CACHE";
        }
        result.admit_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - start).count();
        return result;
    }};
}

} // namespace starrocks::parquet
