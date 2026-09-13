// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "formats/parquet/file_reader.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <functional>

#include "base/uid_util.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/hive/scanner/hdfs_scanner_context.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/rap_index.h"
#include "fs/fs_memory.h"
#include "runtime/rap_build_gate.h"
#include "runtime/runtime_state.h"

namespace starrocks::parquet {

TEST(RapBuildGateReaderTest, FencedReaderFailsBeforeOpeningMetadata) {
    FormatScanContext ctx;
    ctx.rap_build_token = "never-opened-" + print_id(UniqueId::gen_uid());
    // Null file intentionally proves that rejected admission cannot reach I/O.
    FileReader reader(4096, nullptr, 0);
    EXPECT_TRUE(reader.init(&ctx).is_cancelled());
}

TEST(RapBuildGateReaderTest, FailedMetadataRetainsLeaseUntilReaderDestruction) {
    auto& gate = RapBuildGate::instance();
    const auto before = gate.snapshot();
    const std::string token = print_id(UniqueId::gen_uid());
    ASSERT_TRUE(gate.open(before.boot, before.generation, {token, "test", "gs://test/build", "v"}));
    FormatScanContext ctx;
    FormatScannerStats stats;
    ctx.stats = &stats;
    ctx.rap_build_token = token;
    auto file = new_random_access_file_from_memory("bad.parquet", "not-a-parquet-file");
    {
        FileReader reader(4096, file.get(), 18);
        EXPECT_FALSE(reader.init(&ctx).ok());
        EXPECT_EQ(1, gate.snapshot().active_builders);
        EXPECT_TRUE(gate.fence(before.boot, before.generation + 1, token, "test"));
        EXPECT_EQ(1, gate.snapshot().active_builders);
        EXPECT_EQ(nullptr, gate.acquire(token));
    }
    EXPECT_EQ(0, gate.snapshot().active_builders);
}

TEST(RapBuildGateReaderTest, WholeFileBuildUsesAttemptSettingsAndDrainsAfterClose) {
    auto& gate = RapBuildGate::instance();
    const auto before = gate.snapshot();
    const std::string token = print_id(UniqueId::gen_uid());
    const std::string output = (std::filesystem::temp_directory_path() / ("rap_gate_" + token)).string();
    const std::string saved_dir = config::rap_build_index_dir;
    const std::string saved_cols = config::rap_build_index_columns;
    struct Cleanup {
        std::function<void()> run;
        ~Cleanup() { run(); }
    } cleanup{[&] {
        gate.fence(before.boot, before.generation + 1, token, "test");
        config::rap_build_index_dir = saved_dir;
        config::rap_build_index_columns = saved_cols;
        std::error_code error;
        std::filesystem::remove_all(output, error);
    }};
    config::rap_build_index_dir = "";
    config::rap_build_index_columns = "unrelated_column";
    ASSERT_TRUE(gate.open(before.boot, before.generation, {token, "test", output, "c3"}));

    ObjectPool pool;
    FragmentDictState dict;
    RuntimeState runtime{TQueryGlobals()};
    runtime.set_fragment_dict_state(&dict);
    // Use the production context constructor: it binds the empty predicate tree
    // required by FileReader::_filter_group even for an unfiltered whole-file read.
    HdfsScannerContext scanner;
    auto& ctx = scanner.format_scan_context;
    FormatScannerStats stats;
    std::atomic<int32_t> coalesce{0};
    ctx.stats = &stats;
    ctx.timezone = "Asia/Shanghai";
    ctx.lazy_column_coalesce_counter = &coalesce;
    ctx.rap_build_token = token;
    Utils::SlotDesc slots[] = {{"c3", TypeDescriptor::from_logical_type(TYPE_VARCHAR)}, {""}};
    auto* tuple = Utils::create_tuple_descriptor(&runtime, &pool, slots);
    Utils::make_column_info_vector(tuple, &ctx.materialized_columns);
    const std::string path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet2";
    const uint64_t size = std::filesystem::file_size(path);
    ctx.scan_range_offset = 4;
    ctx.scan_range_length = size;
    auto opened = FileSystem::Default()->new_random_access_file(path);
    ASSERT_TRUE(opened.ok());
    auto file = std::move(opened).value();
    {
        FileReader reader(4096, file.get(), size);
        ASSERT_TRUE(reader.init(&ctx).ok());
        ASSERT_TRUE(gate.fence(before.boot, before.generation + 1, token, "test"));
        EXPECT_EQ(1, gate.snapshot().active_builders);
        // An admitted reader finishes under its captured settings after the fence.
        while (true) {
            auto chunk = std::make_shared<Chunk>();
            chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true),
                                 tuple->slots()[0]->id());
            auto status = reader.get_next(&chunk);
            if (status.is_end_of_file()) break;
            ASSERT_TRUE(status.ok()) << status.message();
        }
        EXPECT_EQ(1, stats.rap_build_written) << stats.rap_build_reason;
        EXPECT_EQ(1, gate.snapshot().active_builders);
        const auto key = RapIndex::key_of(file->filename());
        auto index = RapIndex::load(output + "/" + key + ".c3.rapx", {key, size, 10, "c3", -1});
        ASSERT_EQ(RapIndex::State::READY, index.state) << index.reason;
        EXPECT_EQ(4, index.index->num_values());
        for (const std::string value : {"a", "b", "c", "d"}) {
            const auto ranges = index.index->lookup({value});
            ASSERT_EQ(1, ranges.size());
            EXPECT_EQ(0, ranges[0].start_row);
            EXPECT_EQ(10, ranges[0].end_row);
        }
        EXPECT_TRUE(index.index->lookup({"absent"}).empty());
        EXPECT_TRUE(index.index->null_ranges().empty());
    }
    EXPECT_EQ(0, gate.snapshot().active_builders);
    EXPECT_EQ(nullptr, gate.acquire(token));
}

} // namespace starrocks::parquet
