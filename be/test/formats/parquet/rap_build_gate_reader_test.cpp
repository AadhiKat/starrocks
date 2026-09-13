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

#include "base/uid_util.h"
#include "fs/fs_memory.h"
#include "runtime/rap_build_gate.h"

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

} // namespace starrocks::parquet
