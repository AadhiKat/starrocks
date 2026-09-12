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

#include <glog/logging.h>

#include <cstring>
#include <iterator>
#include <limits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/datacache.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/config_scan_io_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "compute_env/runtime_range_pruner.hpp"
#include "exprs/chunk_predicate_evaluator.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "common/config.h"
#include "formats/parquet/rap_index.h"
#include "cache/mem_cache/page_cache.h"
#include "common/runtime_profile.h"
#include "storage_primitive/column_predicate.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"

namespace starrocks::parquet {

FileReader::FileReader(int chunk_size, RandomAccessFile* file, size_t file_size,
                       const DataCacheOptions& datacache_options, SharedBufferedInputStream* sb_stream,
                       SkipRowsContextPtr skip_rows_context)
        : _chunk_size(chunk_size),
          _file(file),
          _file_size(file_size),
          _datacache_options(datacache_options),
          _sb_stream(sb_stream),
          _skip_rows_ctx(std::move(skip_rows_context)) {}

FileReader::~FileReader() = default;

Status FileReader::init(FormatScanContext* ctx) {
    _scanner_ctx = ctx;
    if (ctx->options.use_file_metacache) {
        _cache = DataCache::GetInstance()->page_cache();
    }

    // parse FileMetadata
    FileMetaDataParser file_metadata_parser{_file, ctx, _cache, &_datacache_options, _file_size};
    ASSIGN_OR_RETURN(_file_metadata, file_metadata_parser.get_file_metadata());

    // set existed SlotDescriptor in this parquet file
    std::unordered_set<std::string> existed_column_names;
    _meta_helper = _build_meta_helper();
    _prepare_read_columns(existed_column_names);
    RETURN_IF_ERROR(_scanner_ctx->update_materialized_columns(existed_column_names));
    ASSIGN_OR_RETURN(_is_file_filtered, _scanner_ctx->should_skip_by_evaluating_not_existed_slots());
    if (_is_file_filtered) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_build_split_tasks());
    if (_scanner_ctx->split.split_tasks.size() > 0) {
        _scanner_ctx->split.has_split_tasks = true;
        _is_file_filtered = true;
        return Status::OK();
    }

    if (_scanner_ctx->runtime_filter_scan_range_pruner != nullptr) {
        _runtime_filter_scan_range_pruner =
                std::make_shared<RuntimeScanRangePruner>(*_scanner_ctx->runtime_filter_scan_range_pruner);
    }
    _maybe_consult_rap_index();
    if (_rap_ready && _rap_ranges.empty()) {
        // a READY index says no row of this file matches the EQ / IN literal(s)
        _is_file_filtered = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_init_group_readers());
    return Status::OK();
}


// RAP / lake-index slice m1. Consult a per-file sidecar index for EQ / IN predicates on the
// indexed column, directly under the root AND of the predicate tree, BEFORE any row group is
// prepared. READY -> the matching pages' row ranges (intersected with any transport hint) become
// this file's selected ranges; an empty result means no row of this file matches. ABSENT ->
// nothing. UNUSABLE -> counted, logged, and the scan proceeds unindexed and complete.
void FileReader::_maybe_consult_rap_index() {
    // CX-28: the WHOLE consult is timed -- cache lookup, any load, the postings lookup -- so a
    // warm-cache consult has a measured cost rather than an inferred zero.
    int64_t consult_ns = 0;
    {
        SCOPED_RAW_TIMER(&consult_ns);
        _maybe_consult_rap_index_impl();
    }
    if (_scanner_ctx != nullptr && _scanner_ctx->stats != nullptr) _scanner_ctx->stats->rap_index_consult_ns += consult_ns;
}

void FileReader::_maybe_consult_rap_index_impl() {
    _rap_ready = false;
    _rap_ranges.clear();
    const std::string& dir = config::rap_index_dir;
    if (dir.empty() || _scanner_ctx == nullptr || _scanner_ctx->predicate_tree == nullptr ||
        _scanner_ctx->predicate_tree->empty() || _file_metadata == nullptr) {
        return;
    }
    FormatScannerStats* stats = _scanner_ctx->stats;
    const PredicateTree& tree = *_scanner_ctx->predicate_tree;
    const auto& root = tree.root();
    const auto& cid_to_preds = tree.compound_node_context(root.id()).cid_to_col_preds(root);
    for (const auto& [cid, preds] : cid_to_preds) {
        std::string col;
        for (const auto& mc : _scanner_ctx->materialized_columns) {
            if (mc.slot_id() == static_cast<SlotId>(cid)) {
                col = std::string(mc.name());
                break;
            }
        }
        if (col.empty()) continue;
        std::vector<std::string> values;
        bool supported = false;
        for (const ColumnPredicate* p : preds) {
            if (p == nullptr || p->type_info() == nullptr || !is_string_type(p->type_info()->type())) continue;
            if (p->type() == PredicateType::kEQ) {
                const Datum d = p->value();
                if (d.is_null()) continue;
                values.emplace_back(d.get_slice().to_string());
                supported = true;
            } else if (p->type() == PredicateType::kInList) {
                for (const Datum& d : p->values()) {
                    if (!d.is_null()) values.emplace_back(d.get_slice().to_string());
                }
                supported = true;
            }
        }
        if (!supported) continue;
        std::string base = _file->filename();
        const auto slash = base.find_last_of('/');
        if (slash != std::string::npos) base = base.substr(slash + 1);
        int32_t field_id = -1;
        const auto& schema = _file_metadata->schema();
        const int32_t fidx = schema.get_field_idx_by_column_name(col);
        if (fidx >= 0 && schema.exist_filed_id()) field_id = schema.get_stored_column_by_field_idx(fidx)->field_id;
        if (stats != nullptr) stats->rap_index_consulted++;
        // slice m2a: registered-state reuse. The parsed sidecar is cached in the reader's bounded page
        // cache under the SAME identity StarRocks keys its footer cache with (CacheType::INDEX prefix
        // "ix"). A hit skips the load entirely; a different file, or a rewritten one (size/mtime),
        // misses by construction. UNUSABLE / ABSENT are not cached (re-examined on the next consult).
        std::shared_ptr<RapIndex> index;
        PageCacheHandle rap_cache_handle;
        std::string rap_cache_key, rap_neg_key;
        const bool cache_on = (_cache != nullptr && _scanner_ctx->options.use_file_metacache);
        if (cache_on) {
            // CX-27: the key is per (file identity, column) -- one file may carry indexes on
            // several columns, and a hit must never hand one column's postings to another's predicate.
            // slice 2f: the generation joins the key -- bumping rap_index_generation invalidates every entry
            // slice 2f v2 (CX-45): both keys through RapIndex::cache_key -- disjoint namespaces, length-prefixed fields
            const std::string file_key = ParquetUtils::get_file_cache_key(CacheType::INDEX, _file->filename(),
                                                                          _datacache_options.modification_time, _file_size);
            const std::string generation = std::string(config::rap_index_generation);
            rap_cache_key = RapIndex::cache_key(false, file_key, col, generation, "");
            if (_cache->lookup(rap_cache_key, &rap_cache_handle)) {
                auto cached = *(reinterpret_cast<const std::shared_ptr<RapIndex>*>(rap_cache_handle.data()));
                // CX-27: a cached object is applicable only if its identity matches what the loader
                // would have validated -- same column, compatible field id, same file identity
                const auto& id = cached->identity();
                const bool compatible = id.column == col && id.file_name == base && id.file_size == _file_size &&
                                        id.file_rows == static_cast<uint64_t>(_file_metadata->num_rows()) &&
                                        (field_id < 0 || id.field_id < 0 || id.field_id == field_id);
                if (compatible) {
                    index = cached;
                    if (stats != nullptr) stats->rap_index_cache_hit++;
                } else {
                    if (stats != nullptr) stats->rap_index_cache_incompatible++;
                    // fall through to the load path, which refuses the mismatch and leaves the scan unindexed
                }
            }
            if (index == nullptr) {
                // slice 2f: a refused consult is remembered per DIRECTORY (absence is a fact about a directory, not
                // about the file -- D3-e attempt 1); a hit here skips every filesystem call and repeats the outcome
                rap_neg_key = RapIndex::cache_key(true, file_key, col, generation, dir);
                PageCacheHandle neg_handle;
                if (_cache->lookup(rap_neg_key, &neg_handle)) {
                    const auto* neg = reinterpret_cast<const RapIndex::NegativeEntry*>(neg_handle.data());
                    if (stats != nullptr) {
                        stats->rap_index_negative_hit++;
                        if (neg->unusable) stats->rap_index_unusable++;
                        if (stats->rap_index_reason.empty() && !neg->reason.empty()) {
                            stats->rap_index_reason = std::string(neg->unusable ? "unusable: " : "absent: ") + neg->reason;
                        }
                    }
                    return;
                }
            }
        }
        // slice 2f: remember a refusal under the negative key (small: a flag and the reason)
        auto remember_negative = [&](bool unusable, const std::string& reason) {
            if (!cache_on) return;
            auto deleter = [](const starrocks::CacheKey& key, void* value) { delete (RapIndex::NegativeEntry*)value; };
            MemCacheWriteOptions options;
            options.evict_probability = _datacache_options.datacache_evict_probability;
            auto* entry = new RapIndex::NegativeEntry{unusable, reason};
            PageCacheHandle h;
            Status st = _cache->insert(rap_neg_key, (void*)entry, static_cast<int64_t>(64 + reason.size()), deleter, options, &h);
            if (!st.ok()) delete entry;
        };
        if (index == nullptr) {
            RapIndex::Result res;
            {
                int64_t load_ns = 0;
                {
                    SCOPED_RAW_TIMER(&load_ns);
                    // slice 2c: sidecars are read through the scan's own FileSystem (a gs:// dir works like a
                    // local one); the per-column name <basename>.<column>.rapx is tried first, then the
                    // milestone-1 name <basename>.rapx
                    const RapIndex::Identity expect{base, _file_size, static_cast<uint64_t>(_file_metadata->num_rows()), col, field_id};
                    // slice 2d: the DIRECTORY chooses the filesystem. A local directory (no scheme, or file://)
                    // is read through the default filesystem even when the data file is remote -- the deployed
                    // check's first attempts opened a local dir through the GCS filesystem and judged every
                    // sidecar UNUSABLE. A remote directory (gs://, s3://, hdfs://) keeps slice 2c's behaviour:
                    // the scan's own filesystem, with the data file's credentials.
                    std::string rap_dir = dir;
                    FileSystem* rap_fs = _scanner_ctx->fs;
                    if (rap_dir.find("://") == std::string::npos) {
                        rap_fs = nullptr; // FileSystem::Default()
                    } else if (rap_dir.compare(0, 7, "file://") == 0) {
                        rap_fs = nullptr;
                        rap_dir = rap_dir.substr(7);
                    }
                    res = RapIndex::load(rap_fs, rap_dir + "/" + base + "." + col + RapIndex::kSuffix, expect);
                    if (res.state == RapIndex::State::ABSENT) {
                        res = RapIndex::load(rap_fs, rap_dir + "/" + base + RapIndex::kSuffix, expect);
                    }
                }
                if (stats != nullptr) stats->rap_index_load_ns += load_ns;
            }
            if (res.state == RapIndex::State::ABSENT) {
                // slice 2e: the first non-READY outcome explains itself in the profile (RapIndexConsultReason)
                if (stats != nullptr && stats->rap_index_reason.empty() && !res.reason.empty()) {
                    stats->rap_index_reason = "absent: " + res.reason;
                }
                remember_negative(false, res.reason); // slice 2f
                return;
            }
            if (res.state == RapIndex::State::UNUSABLE) {
                if (stats != nullptr) stats->rap_index_unusable++;
                if (stats != nullptr && stats->rap_index_reason.empty()) stats->rap_index_reason = "unusable: " + res.reason;
                remember_negative(true, res.reason); // slice 2f
                LOG(WARNING) << "RAP index unusable for " << base << " (" << col << "): " << res.reason << "; scanning unindexed";
                return;
            }
            index = std::shared_ptr<RapIndex>(std::move(res.index));
            if (cache_on) {
                if (stats != nullptr) stats->rap_index_cache_miss++;
                auto deleter = [](const starrocks::CacheKey& key, void* value) { delete (std::shared_ptr<RapIndex>*)value; };
                MemCacheWriteOptions options;
                options.evict_probability = _datacache_options.datacache_evict_probability;
                auto capture = std::make_unique<std::shared_ptr<RapIndex>>(index);
                // size estimate: values plus 16 bytes per range plus a small per-value overhead
                const int64_t approx = static_cast<int64_t>(index->approx_bytes());
                Status st = _cache->insert(rap_cache_key, (void*)(capture.get()), approx, deleter, options, &rap_cache_handle);
                if (st.ok()) capture.release();
            }
        }
        if (stats != nullptr) stats->rap_index_ready++;
        auto ranges = index->lookup(values);
        if (!_scanner_ctx->selected_row_ranges.empty()) {
            ranges = RapIndex::intersect(_scanner_ctx->selected_row_ranges, ranges);
        }
        if (stats != nullptr) stats->rap_index_ranges += static_cast<int>(ranges.size());
        _rap_ranges = std::move(ranges);
        _rap_ready = true;
        return; // one indexed column per file in milestone 1
    }
}

std::shared_ptr<MetaHelper> FileReader::_build_meta_helper() {
    if (_scanner_ctx->lake_schema != nullptr && _file_metadata->schema().exist_filed_id()) {
        // Use LakeMetaHelper only when both an Iceberg/Paimon lake schema is present AND
        // the parquet file carries field ids.  Without field ids, the lake schema cannot
        // be matched reliably and we fall back to ParquetMetaHelper which handles
        // col_unique_id / col_physical_name / name lookup chains correctly.
        return std::make_shared<LakeMetaHelper>(_file_metadata.get(), _scanner_ctx->options.case_sensitive,
                                                _scanner_ctx->lake_schema);
    } else {
        return std::make_shared<ParquetMetaHelper>(_file_metadata.get(), _scanner_ctx->options.case_sensitive);
    }
}

const FileMetaData* FileReader::get_file_metadata() {
    return _file_metadata.get();
}

Status FileReader::collect_scan_io_ranges(std::vector<SharedBufferedInputStream::IORange>* io_ranges) {
    int64_t dummy_offset = 0;
    for (auto& r : _row_group_readers) {
        r->collect_io_ranges(io_ranges, &dummy_offset, ColumnIOType::PAGE_INDEX);
        r->collect_io_ranges(io_ranges, &dummy_offset, ColumnIOType::PAGES);
    }
    return Status::OK();
}

Status FileReader::_build_split_tasks() {
    // don't do split in following cases:
    // 1. this feature is not enabled
    // 2. we have already done split before (that's why `split_context` is nullptr)
    if (!_scanner_ctx->options.enable_split_tasks || _scanner_ctx->split_context != nullptr) {
        return Status::OK();
    }

    size_t row_group_size = _file_metadata->t_metadata().row_groups.size();
    for (size_t i = 0; i < row_group_size; i++) {
        const tparquet::RowGroup& row_group = _file_metadata->t_metadata().row_groups[i];
        if (!_select_row_group(row_group)) continue;
        int64_t start_offset = ParquetUtils::get_row_group_start_offset(row_group);
        int64_t end_offset = ParquetUtils::get_row_group_end_offset(row_group);
        if (start_offset >= end_offset) {
            LOG(INFO) << "row group " << i << " is empty. start = " << start_offset << ", end = " << end_offset;
            continue;
        }
#ifndef NDEBUG
        DCHECK(start_offset < end_offset) << "start = " << start_offset << ", end = " << end_offset;
        // there could be holes between row groups.
        // but this does not affect our scan range filter logic.
        // because in `_select_row_group`, we check if `start offset of row group` is in this range
        // so as long as `end_offset > start_offset && end_offset <= start_offset(next_group)`, it's ok
        if ((i + 1) < row_group_size) {
            const tparquet::RowGroup& next_row_group = _file_metadata->t_metadata().row_groups[i + 1];
            DCHECK(end_offset <= ParquetUtils::get_row_group_start_offset(next_row_group));
        }
#endif
        auto split_ctx = std::make_unique<SplitContext>();
        split_ctx->start_offset = start_offset;
        split_ctx->end_offset = end_offset;
        split_ctx->file_metadata = _file_metadata;
        split_ctx->skip_rows_ctx = _skip_rows_ctx;
        _scanner_ctx->split.split_tasks.emplace_back(std::move(split_ctx));
    }
    _scanner_ctx->merge_split_tasks();
    // if only one split task, clear it, no need to do split work.
    if (_scanner_ctx->split.split_tasks.size() <= 1) {
        _scanner_ctx->split.split_tasks.clear();
    }

    if (VLOG_OPERATOR_IS_ON) {
        std::stringstream ss;
        for (const FileScanSplitContextPtr& ctx : _scanner_ctx->split.split_tasks) {
            ss << "[" << ctx->start_offset << "," << ctx->end_offset << "]";
        }
        VLOG_OPERATOR << "FileReader: do_open. split task for " << _file->filename()
                      << ", split_tasks.size = " << _scanner_ctx->split.split_tasks.size() << ", range = " << ss.str();
    }
    return Status::OK();
}

// when doing row group filter, there maybe some error, but we'd better just ignore it instead of returning the error
// status and lead to the query failed.
bool FileReader::_filter_group(const GroupReaderPtr& group_reader) {
    bool& filtered = group_reader->get_is_group_filtered();
    filtered = false;
    // astra CX-11: a row-range hint that selects nothing inside this group filters it
    // outright. This runs before predicate evaluation because it holds regardless of
    // whether any predicate exists -- with no predicates _filter_group() would
    // otherwise keep the group and pass an empty range to page selection.
    if (group_reader->hint_excludes_group()) {
        filtered = true;
        return filtered;
    }
    DCHECK(_scanner_ctx->predicate_tree != nullptr);
    const PredicateTree& predicate_tree = *_scanner_ctx->predicate_tree;
    auto visitor = PredicateFilterEvaluator{predicate_tree, group_reader.get(),
                                            _scanner_ctx->options.parquet_page_index_enable,
                                            _scanner_ctx->options.parquet_bloom_filter_enable};
    auto sparse_range = predicate_tree.visit(visitor);
    _group_reader_param.stats->bloom_filter_tried_counter += visitor.counter.bloom_filter_tried_counter;
    _group_reader_param.stats->bloom_filter_success_counter += visitor.counter.bloom_filter_success_counter;
    _group_reader_param.stats->statistics_tried_counter += visitor.counter.statistics_tried_counter;
    _group_reader_param.stats->statistics_success_counter += visitor.counter.statistics_success_counter;
    _group_reader_param.stats->page_index_tried_counter += visitor.counter.page_index_tried_counter;
    _group_reader_param.stats->page_index_filter_group_counter += visitor.counter.page_index_filter_group_counter;
    _group_reader_param.stats->page_index_success_counter += visitor.counter.page_index_success_counter;
    if (!sparse_range.ok()) {
        LOG(WARNING) << "filter row group failed: " << sparse_range.status().message();
    } else if (sparse_range.value().has_value()) {
        if (sparse_range.value()->span_size() == 0) {
            // no rows selected, the whole row group can be filtered
            filtered = true;
        } else if (sparse_range.value()->span_size() < group_reader->get_row_group_metadata()->num_rows) {
            // some pages have been filtered. astra CX-11: INTERSECT with whatever the
            // row-range transport already selected instead of overwriting it, so the
            // two pruning sources compose.
            group_reader->intersect_range(sparse_range.value().value());
            if (group_reader->get_range().span_size() == 0) {
                // hint and predicate pruning are disjoint: nothing left in this group
                filtered = true;
            }
        }
    }
    return filtered;
}

StatusOr<bool> FileReader::_update_rf_and_filter_group(const GroupReaderPtr& group_reader) {
    bool filter = false;
    if (_runtime_filter_scan_range_pruner != nullptr) {
        RETURN_IF_ERROR(_runtime_filter_scan_range_pruner->update_range_if_arrived(
                _scanner_ctx->global_dictmaps,
                [this, &filter, &group_reader](auto cid, const PredicateList& predicates) {
                    PredicateCompoundNode<CompoundNodeType::AND> pred_tree;
                    for (const auto& pred : predicates) {
                        pred_tree.add_child(PredicateColumnNode{pred});
                    }
                    auto real_tree = PredicateTree::create(std::move(pred_tree));
                    auto visitor = PredicateFilterEvaluator{real_tree, group_reader.get(), false, false};
                    auto res = real_tree.visit(visitor);
                    if (res.ok() && res->has_value() && res->value().span_size() == 0) {
                        filter = true;
                    }
                    this->_group_reader_param.stats->bloom_filter_tried_counter +=
                            visitor.counter.bloom_filter_tried_counter;
                    this->_group_reader_param.stats->bloom_filter_success_counter +=
                            visitor.counter.bloom_filter_success_counter;
                    this->_group_reader_param.stats->statistics_tried_counter +=
                            visitor.counter.statistics_tried_counter;
                    this->_group_reader_param.stats->statistics_success_counter +=
                            visitor.counter.statistics_success_counter;
                    this->_group_reader_param.stats->page_index_tried_counter +=
                            visitor.counter.page_index_tried_counter;
                    this->_group_reader_param.stats->page_index_filter_group_counter +=
                            visitor.counter.page_index_filter_group_counter;
                    this->_group_reader_param.stats->page_index_success_counter +=
                            visitor.counter.page_index_success_counter;
                    return Status::OK();
                },
                true, 0));
    }
    return filter;
}

void FileReader::_prepare_read_columns(std::unordered_set<std::string>& existed_column_names) {
    _meta_helper->prepare_read_columns(_scanner_ctx->materialized_columns, &_scanner_ctx->column_access_paths,
                                       _group_reader_param.read_cols, existed_column_names);
    _no_materialized_column_scan =
            (_group_reader_param.read_cols.empty() && _scanner_ctx->reserved_field_slots.empty());
}

bool FileReader::_select_row_group(const tparquet::RowGroup& row_group) {
    size_t row_group_start = ParquetUtils::get_row_group_start_offset(row_group);
    size_t scan_start = _scanner_ctx->scan_range_offset;
    size_t scan_end = _scanner_ctx->scan_range_length + scan_start;
    if (row_group_start >= scan_start && row_group_start < scan_end) {
        return true;
    }
    return false;
}

Status FileReader::_collect_row_group_io(std::shared_ptr<GroupReader>& group_reader) {
    // collect io ranges.
    if (config::parquet_coalesce_read_enable && _sb_stream != nullptr) { //should move to scanner_ctx
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        ColumnIOTypeFlags flags = 0;
        if (_scanner_ctx->options.parquet_page_index_enable) {
            flags |= ColumnIOType::PAGE_INDEX;
        }
        if (_scanner_ctx->options.parquet_bloom_filter_enable) {
            flags |= ColumnIOType::BLOOM_FILTER;
        }
        group_reader->collect_io_ranges(&ranges, &end_offset, flags);
        RETURN_IF_ERROR(_sb_stream->set_io_ranges(ranges));
    }
    return Status::OK();
}

Status FileReader::_init_group_readers() {
    // _group_reader_param is used by all group readers.
    // scanner_ctx replaces 11 individual field copies; GroupReader accesses
    // context-derived data (timezone, options, partitions, slots, dicts, etc.)
    // through the pointer.  File-infrastructure fields (sb_stream, file, etc.)
    // and hot fields (stats, lazy_column_coalesce_counter) remain direct copies.
    _group_reader_param.scan_ctx = _scanner_ctx;
    _group_reader_param.conjunct_ctxs_by_slot = _scanner_ctx->conjunct_ctxs_by_slot;
    _group_reader_param.stats = _scanner_ctx->stats;
    _group_reader_param.sb_stream = _sb_stream;
    _group_reader_param.chunk_size = _chunk_size;
    _group_reader_param.file = _file;
    _group_reader_param.file_metadata = _file_metadata.get();
    _group_reader_param.lazy_column_coalesce_counter = _scanner_ctx->lazy_column_coalesce_counter;
    _group_reader_param.modification_time = _datacache_options.modification_time;
    _group_reader_param.file_size = _file_size;
    _group_reader_param.datacache_options = &_datacache_options;
    _group_reader_param.scan_range_id = _scanner_ctx->scan_range_id;
    // RAP row-range transport: empty stays nullptr so the no-hint path is untouched.
    _group_reader_param.selected_row_ranges =
            _scanner_ctx->selected_row_ranges.empty() ? nullptr : &_scanner_ctx->selected_row_ranges;
    // RAP slice m1: a READY index's ranges (already intersected with any transport hint) take over.
    if (_rap_ready && !_rap_ranges.empty()) {
        _group_reader_param.selected_row_ranges = &_rap_ranges;
    }

    int64_t row_group_first_row = 0;
    // select and create row group readers.
    for (size_t i = 0; i < _file_metadata->t_metadata().row_groups.size(); i++) {
        if (i > 0) {
            row_group_first_row += _file_metadata->t_metadata().row_groups[i - 1].num_rows;
        }

        if (!_select_row_group(_file_metadata->t_metadata().row_groups[i])) {
            continue;
        }

        if (_file_metadata->t_metadata().row_groups[i].num_rows == 0) {
            continue;
        }

        auto row_group_reader =
                std::make_shared<GroupReader>(_group_reader_param, i, _skip_rows_ctx, row_group_first_row);
        RETURN_IF_ERROR(row_group_reader->init());

        _group_reader_param.stats->total_row_groups += 1;

        RETURN_IF_ERROR(_collect_row_group_io(row_group_reader));
        // You should call row_group_reader->init() before _filter_group()
        if (_filter_group(row_group_reader)) {
            DLOG(INFO) << "row group " << i << " of file has been filtered";
            _group_reader_param.stats->filtered_row_groups += 1;
            continue;
        }

        _row_group_readers.emplace_back(row_group_reader);
        int64_t num_rows = _file_metadata->t_metadata().row_groups[i].num_rows;
        // for skip rows which already deleted
        if (_skip_rows_ctx != nullptr && _skip_rows_ctx->has_skip_rows()) {
            uint64_t deletion_rows = _skip_rows_ctx->deletion_bitmap->get_range_cardinality(
                    row_group_first_row, row_group_first_row + num_rows);
            num_rows -= deletion_rows;
        }
        _total_row_count += num_rows;
    }
    _row_group_size = _row_group_readers.size();

    if (!_row_group_readers.empty()) {
        // prepare first row group
        RETURN_IF_ERROR(_row_group_readers[_cur_row_group_idx]->prepare());
    }

    return Status::OK();
}

Status FileReader::get_next(ChunkPtr* chunk) {
    if (_is_file_filtered) {
        return Status::EndOfFile("");
    }
    if (_no_materialized_column_scan) {
        RETURN_IF_ERROR(_exec_no_materialized_column_scan(chunk));
        return Status::OK();
    }

    if (_cur_row_group_idx < _row_group_size) {
        size_t row_count = _chunk_size;
        Status status;
        try {
            status = _row_group_readers[_cur_row_group_idx]->get_next(chunk, &row_count);
        } catch (std::exception& e) {
            return Status::InternalError(
                    strings::Substitute("Encountered Exception while reading. reason = $0", e.what()));
        }
        if (status.ok() || status.is_end_of_file()) {
            if (row_count > 0) {
                // partition / not-existed / extended columns are now appended
                // inside GroupReader::get_next() before emit_physical_columns.
                _scan_row_count += (*chunk)->num_rows();
            }
            if (status.is_end_of_file()) {
                // release previous RowGroupReader
                do {
                    _row_group_readers[_cur_row_group_idx] = nullptr;
                    _cur_row_group_idx++;
                    if (_cur_row_group_idx < _row_group_size) {
                        const auto& cur_row_group = _row_group_readers[_cur_row_group_idx];
                        auto ret = _update_rf_and_filter_group(cur_row_group);
                        if (ret.ok() && ret.value()) {
                            // row group is filtered by runtime filter
                            _group_reader_param.stats->filtered_row_groups += 1;
                            continue;
                        } else if (ret.status().is_end_of_file()) {
                            // If rf is always false, will return eof
                            _group_reader_param.stats->filtered_row_groups += (_row_group_size - _cur_row_group_idx);
                            _row_group_readers.assign(_row_group_readers.size(), nullptr);
                            _cur_row_group_idx = _row_group_size;
                            break;
                        } else {
                            // do nothing, ignore the error code
                        }

                        RETURN_IF_ERROR(cur_row_group->prepare());
                    }
                    break;
                } while (true);

                return Status::OK();
            }
        } else {
            auto s = strings::Substitute("FileReader::get_next failed. reason = $0, file = $1", status.to_string(),
                                         _file->filename());
            LOG(WARNING) << s;
            return status;
        }

        return status;
    }

    return Status::EndOfFile("");
}

Status FileReader::_exec_no_materialized_column_scan(ChunkPtr* chunk) {
    if (_scan_row_count < _total_row_count) {
        size_t read_size = 0;
        if (_scanner_ctx->options.use_count_opt) {
            read_size = _total_row_count - _scan_row_count;
            // append_side_columns_to_chunk fills per-row count (value=1),
            // partition, and extended columns first.  The next call overwrites
            // the count column with the aggregated file row count.
            RETURN_IF_ERROR(_scanner_ctx->append_side_columns_to_chunk(chunk, 1));
            _scanner_ctx->append_or_update_count_column_to_chunk(chunk, 1, read_size);
        } else {
            read_size = std::min(static_cast<size_t>(_chunk_size), _total_row_count - _scan_row_count);
            RETURN_IF_ERROR(_scanner_ctx->append_side_columns_to_chunk(chunk, read_size));
        }
        _scan_row_count += read_size;
        if (!_scanner_ctx->conjuncts.scanner_ctxs.empty()) {
            RETURN_IF_ERROR(
                    ChunkPredicateEvaluator::eval_conjuncts(_scanner_ctx->conjuncts.scanner_ctxs, (*chunk).get()));
        }
        return Status::OK();
    }

    return Status::EndOfFile("");
}

} // namespace starrocks::parquet
