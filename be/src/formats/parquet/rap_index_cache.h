// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#pragma once

#include "cache/cache_options.h"
#include "cache/mem_cache/page_cache.h"
#include "formats/parquet/rap_index.h"
#include "runtime/rap_index_preloader.h"

namespace starrocks::parquet {

// One cache ABI for demand readers and preloaders. Both own shared_ptr<RapIndex>
// entries and validate them against the actual file before using any postings.
class RapIndexCache {
public:
    static std::string key(const std::string& file_uri, uint64_t size, int64_t mtime,
                           const std::string& column, const std::string& generation);
    static bool compatible(const RapIndex& index, const RapIndex::Identity& expected);
    static std::shared_ptr<RapIndex> lookup(StoragePageCache* cache, const std::string& key,
                                            const RapIndex::Identity& expected);
    static Status insert(StoragePageCache* cache, const std::string& key, std::shared_ptr<RapIndex> index,
                         MemCacheWriteOptions options = {});
    // Caller retains the filesystem lifetime through the load. No borrowed query state.
    static RapIndexPreloader::Prepared prepare(StoragePageCache* cache, FileSystem* fs,
                                               const RapIndexPreloader::Request& request);
    static bool resident(StoragePageCache* cache, const RapIndexPreloader::Request& request);
};

} // namespace starrocks::parquet
