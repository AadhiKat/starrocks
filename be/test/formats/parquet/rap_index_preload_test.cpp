// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "formats/parquet/rap_index_cache.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "column/binary_column.h"
#include "formats/parquet/rap_sidecar_builder.h"
#include "formats/parquet/utils.h"
#include "fs/fs_memory.h"

namespace starrocks::parquet {

class RapIndexPreloadTest : public testing::Test {
protected:
    class CountingFs : public MemoryFileSystem {
    public:
        Status path_exists(const std::string& path) override {
            ++probes;
            return MemoryFileSystem::path_exists(path);
        }
        int probes = 0;
    } fs;
    LRUCacheEngine engine;
    StoragePageCache cache{&engine};
    RapIndexPreloader::Request r;
    std::string bytes;
    std::string path;

    void SetUp() override {
        MemCacheOptions options;
        options.mem_space_size = 16 * 1024 * 1024;
        ASSERT_OK(engine.init(options));
        r = {"a", "gs://bucket/table/data/file.parquet", "/indices", "g1", "model", 100, 3, 0, 3, 1024 * 1024};
        auto values = BinaryColumn::create();
        values->append(Slice("a")); values->append(Slice("b")); values->append(Slice("b"));
        formats::RapSidecarBuilder builder("model", 3);
        builder.observe(*values, 0);
        bytes = builder.encode(RapIndex::key_of(r.file_uri), r.file_size, r.file_rows);
        path = builder.sidecar_path(r.directory, RapIndex::key_of(r.file_uri));
        ASSERT_OK(fs.create_dir_recursive(path.substr(0, path.rfind('/'))));
        write(bytes);
    }
    void write(const std::string& value) {
        auto f = fs.new_writable_file(path);
        ASSERT_TRUE(f.ok());
        ASSERT_OK((*f)->append(Slice(value)));
        ASSERT_OK((*f)->close());
    }
};

TEST_F(RapIndexPreloadTest, RealBuilderLoaderAndDemandCacheShareEntry) {
    auto prepared = RapIndexCache::prepare(&cache, &fs, r);
    ASSERT_TRUE(prepared.admit);
    EXPECT_FALSE(RapIndexCache::resident(&cache, r));
    EXPECT_EQ(bytes.size(), prepared.result.bytes);
    EXPECT_EQ(1, prepared.result.attempts);
    EXPECT_EQ("LOADED", prepared.admit().state);
    EXPECT_TRUE(RapIndexCache::resident(&cache, r));
    const auto demand_key = RapIndex::cache_key(false,
            ParquetUtils::get_file_cache_key(CacheType::INDEX, r.file_uri, r.modification_time, r.file_size),
            r.column, r.generation, "");
    EXPECT_EQ(demand_key, RapIndexCache::key(r.file_uri, r.file_size, r.modification_time, r.column, r.generation));
    auto index = RapIndexCache::lookup(&cache, demand_key,
            {RapIndex::key_of(r.file_uri), r.file_size, r.file_rows, r.column, r.field_id});
    ASSERT_NE(nullptr, index);
    auto rows = index->lookup({"b"});
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(0, rows[0].start_row);
    EXPECT_EQ(3, rows[0].end_row);
    EXPECT_TRUE(index->lookup({"absent"}).empty());
    const int probes = fs.probes;
    auto again = RapIndexCache::prepare(&cache, &fs, r);
    EXPECT_EQ("CACHED", again.result.state);
    EXPECT_FALSE(again.admit);
    EXPECT_EQ(probes, fs.probes); // A real warm path, not a second remote load.
}

TEST_F(RapIndexPreloadTest, MissingCanonicalDoesNotProbeLegacyOrPoisonNegativeCache) {
    ASSERT_OK(fs.delete_file(path));
    auto missing = RapIndexCache::prepare(&cache, &fs, r);
    EXPECT_EQ("ABSENT", missing.result.state);
    EXPECT_FALSE(missing.admit);
    EXPECT_EQ(1, fs.probes);
    EXPECT_EQ(1, missing.result.attempts);
    EXPECT_EQ(0, missing.result.bytes);
    EXPECT_EQ(0, missing.result.open_ns);
    EXPECT_EQ(0, missing.result.size_ns);
    EXPECT_EQ(0, missing.result.read_ns);
    EXPECT_EQ(0, missing.result.parse_ns);
    write(bytes);
    auto available = RapIndexCache::prepare(&cache, &fs, r);
    ASSERT_TRUE(available.admit);
    EXPECT_EQ("LOADED", available.admit().state);
}

TEST_F(RapIndexPreloadTest, CorruptOrOversizedSidecarCannotEnterCache) {
    auto corrupted = bytes;
    corrupted[24] ^= 1;
    write(corrupted);
    auto invalid = RapIndexCache::prepare(&cache, &fs, r);
    EXPECT_EQ("UNUSABLE", invalid.result.state);
    EXPECT_FALSE(invalid.admit);
    EXPECT_FALSE(RapIndexCache::resident(&cache, r));
    write(bytes);
    r.max_bytes = bytes.size() - 1;
    auto capped = RapIndexCache::prepare(&cache, &fs, r);
    EXPECT_EQ("UNUSABLE", capped.result.state);
    EXPECT_EQ(0, capped.result.bytes);
    EXPECT_EQ(0, capped.result.read_ns);
    EXPECT_FALSE(capped.admit);
}

TEST_F(RapIndexPreloadTest, FileSchemaAndGenerationMustMatchAtConsumption) {
    auto prepared = RapIndexCache::prepare(&cache, &fs, r);
    ASSERT_TRUE(prepared.admit);
    ASSERT_EQ("LOADED", prepared.admit().state);
    for (int field = 0; field != 6; ++field) {
        auto wrong = r;
        if (field == 0) wrong.file_size++;
        if (field == 1) wrong.file_rows++;
        if (field == 2) wrong.field_id++;
        if (field == 3) wrong.column = "other";
        if (field == 4) wrong.generation = "g2";
        if (field == 5) wrong.file_uri = "gs://other/table/data/file.parquet";
        EXPECT_FALSE(RapIndexCache::resident(&cache, wrong)) << field;
    }
    auto wrong = r; wrong.file_rows++;
    auto mismatch = RapIndexCache::prepare(&cache, &fs, wrong);
    EXPECT_EQ("UNUSABLE", mismatch.result.state);
    EXPECT_FALSE(mismatch.admit);
    EXPECT_TRUE(RapIndexCache::resident(&cache, r));
    // A completed receipt is not a pin. Drop the prepared closure before eviction.
    prepared.admit = {};
    cache.prune();
    EXPECT_FALSE(RapIndexCache::resident(&cache, r));
}

TEST_F(RapIndexPreloadTest, UnavailableCacheDoesNotFetch) {
    StoragePageCache unavailable;
    auto result = RapIndexCache::prepare(&unavailable, &fs, r);
    EXPECT_EQ("NO_CACHE", result.result.state);
    EXPECT_EQ(0, fs.probes);
    EXPECT_FALSE(result.admit);
}

} // namespace starrocks::parquet
