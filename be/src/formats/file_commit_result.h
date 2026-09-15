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

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"

namespace starrocks::formats {

struct FileStatistics {
    int64_t record_count = 0;
    int64_t file_size = 0;
    std::optional<std::vector<int64_t>> split_offsets;
    std::optional<std::map<int32_t, int64_t>> column_sizes;
    std::optional<std::map<int32_t, int64_t>> value_counts;
    std::optional<std::map<int32_t, int64_t>> null_value_counts;
    std::optional<std::map<int32_t, std::string>> lower_bounds;
    std::optional<std::map<int32_t, std::string>> upper_bounds;
};

// RAP / lake-index: what the sidecar builders did while this file was written. It rides on the commit result because
// that is the one object that reaches the sink from the writer, and the sink is where the counters belong: the
// scan-side `RapBuild*` counters live in CONNECTOR_SCAN and read 0 during an INSERT's own source scan while the sink
// writes sidecars (`s9-export-cost-is-free.md`), so before this there was no way to tell from a profile whether an
// export had built its index -- only by listing the object store afterwards.
struct RapExportStats {
    int64_t attempted = 0;        // builders that reached the write decision for this file
    int64_t sidecars_written = 0; // sidecars actually written
    int64_t sidecar_bytes = 0;    // their encoded bytes
    int64_t failures = 0;         // attempted but not written (over the distinct-value ceiling, or the write failed)
    int64_t build_ns = 0;         // encode + write, measured around the whole sidecar pass at close
};

struct FileCommitResult {
    Status io_status;
    std::string format;
    FileStatistics file_statistics;
    std::string location;
    std::function<void()> rollback_action;
    RapExportStats rap_index;
};

} // namespace starrocks::formats
