// RAP slice milestone 1 -- the sidecar index against the REAL fixture files, through the real
// FileReader. Pre-registered acceptance in slice-m1.md section 3.
//
// Fixture files (read-only copies of the two data files of ice_poc.poc_lake.pg_n5000000, snapshot
// 6366882456050597382) are expected under RAP_FIXTURE_DIR (default /root/fixtures/n5m) and their
// sidecars under RAP_INDEX_DIR (default /root/fixtures/n5m/idx). Tests that need them SKIP when
// they are absent, so the suite is runnable on any tree; the gate test needs nothing.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <unistd.h>

#include "base/hash/crc32c.h"
#include "cache/datacache.h"
#include "cache/mem_cache/local_mem_cache_engine.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/hive/scanner/hdfs_scanner.h"
#include "exec/exec_env.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "formats/parquet/rap_index.h"
#include "formats/parquet/utils.h"
#include "formats/deletion_bitmap.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"
#include <functional>
#include <limits>
#include <optional>
#include "fs/fs.h"
#include "base/string/slice.h"
#include "fs/fs_memory.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
// slice 4 fix-up 5: the scan-side builder's attach test is about the runtime-filter pruner the real scanner builds,
// so the test has to build one exactly as HdfsScanner::_build_scanner_context() does
#include "compute_env/runtime_range_pruner.hpp"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "runtime/runtime_filter.h"
#include "storage_primitive/predicate_parser.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks::parquet {

static FormatScannerStats g_rap_stats;
using starrocks::HdfsScannerContext;

namespace {

std::string env_or(const char* k, const char* d) {
    const char* v = std::getenv(k);
    return (v != nullptr && *v != '\0') ? std::string(v) : std::string(d);
}

const std::string kFile0 = "01a08945-cedd-7f33-a6e9-278f7b4df37d_0_30_0.parquet";
const std::string kFile1 = "01a08945-cedd-7f33-a6e9-278f7b4df37d_0_30_1.parquet";

// slice 2d: filesystems that answer every open the way the HDFS-backed GCS filesystem does for a missing
// object (REMOTE_FILE_NOT_FOUND), and one that fails with a genuine IO error
class RemoteNotFoundFs final : public MemoryFileSystem {
public:
    // slice 2e: existence is asked before the open; this stub has no sidecar, so NotFound, counted
    Status path_exists(const std::string& url) override {
        ++exists_calls;
        return Status::NotFound(url);
    }
    int exists_calls = 0;
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++opens;
        return Status::RemoteFileNotFound("hdfsOpenFile failed, backend=gs://bucket, file=" + url);
    }
    int opens = 0;
};
// an in-memory filesystem addressed through a scheme, standing for a remote store: slice 2d routes a schemed
// directory to the scan's filesystem, so the 2c case's memory-only sidecar must live behind a scheme, not a
// local-looking path (which slice 2d now correctly reads through the default filesystem)
class SchemedMemoryFs final : public MemoryFileSystem {
public:
    Status path_exists(const std::string& url) override { // slice 2e: same scheme handling as the open
        return MemoryFileSystem::path_exists(url.compare(0, 6, "mem://") == 0 ? url.substr(6) : url);
    }
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++opens;
        return MemoryFileSystem::new_random_access_file(opts, url.compare(0, 6, "mem://") == 0 ? url.substr(6) : url);
    }
    int opens = 0;
};
// a filesystem whose every open is a plain NotFound (what the local and in-memory filesystems answer)
class NotFoundFs final : public MemoryFileSystem {
public:
    Status path_exists(const std::string& url) override { // slice 2e: absence is answered here, before any open
        ++exists_calls;
        return Status::NotFound(url);
    }
    int exists_calls = 0;
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++opens;
        return Status::NotFound("no such sidecar: " + url);
    }
    int opens = 0;
};
class IoErrorFs final : public MemoryFileSystem {
public:
    Status path_exists(const std::string& url) override { return Status::OK(); } // slice 2e: the object exists; the OPEN fails
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        return Status::IOError("connector failure opening " + url);
    }
};

// slice 2e: what the deployed HDFS-backed GCS filesystem really does for a missing object -- the open is LAZY and
// succeeds; the first size/read fails with a plain IOError (fs_hdfs.cpp getSize -> hdfsGetPathInfo == nullptr);
// path_exists answers NotFound. The loader must never reach the read for a missing object.
class FailingStream final : public io::SeekableInputStream {
public:
    explicit FailingStream(std::string url) : _url(std::move(url)) {}
    StatusOr<int64_t> read(void* data, int64_t count) override { return Status::IOError("Fail to get path info of " + _url); }
    Status skip(int64_t count) override { return Status::IOError("Fail to get path info of " + _url); }
    Status seek(int64_t position) override { return Status::IOError("Fail to get path info of " + _url); }
    StatusOr<int64_t> position() override { return 0; }
    StatusOr<int64_t> get_size() override { return Status::IOError("Fail to get path info of " + _url + ": No such file or directory"); }

private:
    std::string _url;
};
class LazyMissingFs final : public MemoryFileSystem {
public:
    Status path_exists(const std::string& url) override {
        ++exists_calls;
        return Status::NotFound(url);
    }
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++opens;
        return std::make_unique<RandomAccessFile>(std::make_shared<FailingStream>(url), url);
    }
    int exists_calls = 0, opens = 0;
};
// the object exists (or a race made it vanish after the check): open succeeds lazily, the read fails
class LazyReadFailFs final : public MemoryFileSystem {
public:
    Status path_exists(const std::string& url) override { return Status::OK(); }
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        return std::make_unique<RandomAccessFile>(std::make_shared<FailingStream>(url), url);
    }
};
// slice 4 fix-up 2 (m38 review, PRD-02): what the HDFS-backed and S3 filesystems really do -- get_file_size answers
// NotSupported, the open succeeds, and the STREAM knows the size. The stream serves the given bytes only when the size
// it reports is their true size; a stream reporting any other size fails every read with a sentinel, so a loader that
// touched the payload before checking the reported size is caught. A negative reported size stands for a stream whose
// size cannot be established (get_size fails).
class SentinelStream final : public io::SeekableInputStream {
public:
    SentinelStream(std::string bytes, int64_t reported) : _bytes(std::move(bytes)), _reported(reported) {}
    StatusOr<int64_t> read(void* data, int64_t count) override {
        ++reads;
        if (_reported != static_cast<int64_t>(_bytes.size())) return Status::IOError("SENTINEL: payload read before the size check");
        const int64_t n = std::min<int64_t>(count, static_cast<int64_t>(_bytes.size()) - _pos);
        if (n > 0) std::memcpy(data, _bytes.data() + _pos, static_cast<size_t>(n));
        _pos += std::max<int64_t>(n, 0);
        return std::max<int64_t>(n, 0);
    }
    Status skip(int64_t count) override { _pos += count; return Status::OK(); }
    Status seek(int64_t position) override { _pos = position; return Status::OK(); }
    StatusOr<int64_t> position() override { return _pos; }
    StatusOr<int64_t> get_size() override {
        if (_reported < 0) return Status::IOError("stat failed: size unknown");
        return _reported;
    }
    int reads = 0;

private:
    std::string _bytes;
    int64_t _reported;
    int64_t _pos = 0;
};
class NoSizeFs final : public MemoryFileSystem {
public:
    NoSizeFs(std::string bytes, int64_t reported) : stream(std::make_shared<SentinelStream>(std::move(bytes), reported)) {}
    Status path_exists(const std::string& url) override { return Status::OK(); }
    StatusOr<uint64_t> get_file_size(const std::string& url) override {
        ++size_calls;
        return Status::NotSupported("HdfsFileSystem::get_file_size");
    }
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++opens;
        return std::make_unique<RandomAccessFile>(stream, url);
    }
    std::shared_ptr<SentinelStream> stream;
    int size_calls = 0, opens = 0;
};
// a filesystem whose existence check itself fails with something other than NotFound
class ExistsIoErrorFs final : public MemoryFileSystem {
public:
    Status path_exists(const std::string& url) override { return Status::IOError("permission denied: " + url); }
    using MemoryFileSystem::new_random_access_file;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++opens;
        return std::make_unique<RandomAccessFile>(std::make_shared<FailingStream>(url), url);
    }
    int opens = 0;
};

// Minimal RAPX v1 encoder mirroring harness/rap_index_build.py, for synthetic gate cases.
struct Enc {
    std::string b;
    template <typename T>
    void put(T v) {
        b.append(reinterpret_cast<const char*>(&v), sizeof(T));
    }
    void bytes(const std::string& s) {
        put<uint32_t>(s.size());
        b += s;
    }
};

std::string encode(const std::string& name, uint64_t size, uint64_t rows, const std::string& col, int32_t field_id,
                   uint32_t gran, const std::vector<std::pair<std::string, std::vector<std::pair<int64_t, int64_t>>>>& postings) {
    Enc e;
    e.b.append("RAPX", 4);
    e.put<uint32_t>(RapIndex::kVersion);
    e.put<uint64_t>(size);
    e.put<uint64_t>(rows);
    e.bytes(name);
    e.bytes(col);
    e.put<int32_t>(field_id);
    e.put<uint32_t>(gran);
    e.put<uint32_t>(postings.size());
    e.put<uint64_t>(e.b.size() + 8);
    for (const auto& [v, rs] : postings) {
        e.bytes(v);
        e.put<uint32_t>(rs.size());
        for (const auto& [s, en] : rs) {
            e.put<int64_t>(s);
            e.put<int64_t>(en);
        }
    }
    const uint32_t crc = starrocks::crc32c::Value(e.b.data(), e.b.size());
    e.put<uint32_t>(crc);
    e.b.append("RAPX", 4);
    return e.b;
}

// slice 4: RAPX v2 -- typed keys and a null posting (the layout RapSidecarBuilder writes; the reference encoder mirrors it)
std::string encode_v2(const std::string& name, uint64_t size, uint64_t rows, const std::string& col, int32_t field_id,
                      uint8_t key_type, uint32_t gran,
                      const std::vector<std::pair<std::string, std::vector<std::pair<int64_t, int64_t>>>>& postings,
                      const std::vector<std::pair<int64_t, int64_t>>& null_ranges) {
    Enc e;
    e.b.append("RAPX", 4);
    e.put<uint32_t>(RapIndex::kVersionV2);
    e.put<uint64_t>(size);
    e.put<uint64_t>(rows);
    e.bytes(name);
    e.bytes(col);
    e.put<int32_t>(field_id);
    e.put<uint8_t>(key_type);
    e.put<uint32_t>(gran);
    e.put<uint32_t>(postings.size());
    e.put<uint32_t>(null_ranges.size());
    e.put<uint64_t>(e.b.size() + 8);
    for (const auto& [v, rs] : postings) {
        e.bytes(v);
        e.put<uint32_t>(rs.size());
        for (const auto& [s, en] : rs) {
            e.put<int64_t>(s);
            e.put<int64_t>(en);
        }
    }
    for (const auto& [s, en] : null_ranges) {
        e.put<int64_t>(s);
        e.put<int64_t>(en);
    }
    const uint32_t crc = starrocks::crc32c::Value(e.b.data(), e.b.size());
    e.put<uint32_t>(crc);
    e.b.append("RAPX", 4);
    return e.b;
}

std::string enc_i64(int64_t v) {
    std::string k;
    RapIndex::encode_int64(v, &k);
    return k;
}

// the row encoding's fields ("V<len>:<bytes>;" / "N;"), decoded; a null field is std::nullopt
std::vector<std::optional<std::string>> split_fields(const std::string& row) {
    std::vector<std::optional<std::string>> out;
    size_t o = 0;
    while (o < row.size()) {
        if (row[o] == 'N') {
            out.emplace_back(std::nullopt);
            o += 2;
            continue;
        }
        const size_t colon = row.find(':', o);
        const size_t len = std::stoul(row.substr(o + 1, colon - o - 1));
        out.emplace_back(row.substr(colon + 1, len));
        o = colon + 1 + len + 1;
    }
    return out;
}

std::string with_crc(std::string b) {
    const uint32_t crc = starrocks::crc32c::Value(b.data(), b.size() - 8);
    std::memcpy(b.data() + b.size() - 8, &crc, 4);
    return b;
}

// a path inside a memory filesystem with its parent directories created (keyed names carry directories)
std::string mem_path(MemoryFileSystem& mem, const std::string& root, const std::string& rel) {
    const std::string full = root + "/" + rel;
    const auto psl = full.find_last_of('/');
    (void)mem.create_dir_recursive(full.substr(0, psl));
    return full;
}

// Exact, self-delimiting row encoding (astra CX-26): a null field is the tag "N;", a present field
// is "V<len>:<bytes>;". A null and the string "NULL" differ; embedded separators cannot move a field
// boundary; the multiset of encoded rows is the multiset of typed nullable tuples.
std::string encode_field(bool is_null, const std::string& s) {
    if (is_null) return "N;";
    return "V" + std::to_string(s.size()) + ":" + s + ";";
}

// Sanitized comparison: reports sizes and the first differing position, never row contents.
bool same_multiset(std::vector<std::string> a, std::vector<std::string> b, std::string* diag) {
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    if (a.size() != b.size()) {
        *diag = "sizes differ: " + std::to_string(a.size()) + " vs " + std::to_string(b.size());
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i] != b[i]) {
            *diag = "first differing sorted position " + std::to_string(i) + " of " + std::to_string(a.size());
            return false;
        }
    }
    *diag = "equal (" + std::to_string(a.size()) + " rows)";
    return true;
}

} // namespace

static std::string rekey_sidecar(const std::string& b, const std::string& new_name);

class RapIndexTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
        _fragment_dict_state = std::make_unique<FragmentDictState>();
        _runtime_state->set_fragment_dict_state(_fragment_dict_state.get());
        _fixture_dir = env_or("RAP_FIXTURE_DIR", "/root/fixtures/n5m/data"); // slice 2g v2: under a data/ root
        _legacy_index_dir = env_or("RAP_INDEX_DIR", "/root/fixtures/n5m/idx");
        _saved_dir = config::rap_index_dir;
        _saved_build_dir = config::rap_build_index_dir;
        _saved_build_cols = config::rap_build_index_columns;
        // slice 2g v3 (PRD-01): the fixture sidecars on disk are keyed by basename (images 1-4); the reader now keys a
        // file by its FULL path, so a per-process mirror re-keys them under <mirror>/<key>.rapx (and .model.rapx where
        // present). Tests that need the raw bytes read them from the mirror: the stored name there is the key.
        namespace fs = std::filesystem;
        _mirror = (fs::temp_directory_path() / ("rap_v3_" + std::to_string(::getpid()))).string();
        for (const std::string& f : {kFile0, kFile1}) {
            for (const std::string suffix : {std::string(RapIndex::kSuffix), ".model" + std::string(RapIndex::kSuffix)}) {
                const fs::path src = fs::path(_legacy_index_dir) / (f + suffix);
                if (!fs::exists(src)) continue;
                std::ifstream in(src.string(), std::ios::binary);
                const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
                const std::string k = RapIndex::key_of(_fixture_dir + "/" + f);
                const fs::path dst = fs::path(_mirror) / (k + suffix);
                fs::create_directories(dst.parent_path());
                const std::string keyed = rekey_sidecar(bytes, k);
                std::ofstream out(dst.string(), std::ios::binary);
                out.write(keyed.data(), static_cast<std::streamsize>(keyed.size()));
            }
        }
        _index_dir = _mirror;
    }
    void TearDown() override {
        config::rap_index_dir = _saved_dir;
        config::rap_build_index_dir = _saved_build_dir;
        config::rap_build_index_columns = _saved_build_cols;
        std::error_code ec;
        std::filesystem::remove_all(_mirror, ec); // my own per-process mirror
    }

    // slice 2g v3: the key of a fixture file, and its sidecar in the mirror (per-column name when `col` is given)
    std::string key(const std::string& file) const { return RapIndex::key_of(_fixture_dir + "/" + file); }
    std::string sidecar_of(const std::string& file, const std::string& col = "") const {
        return _index_dir + "/" + key(file) + (col.empty() ? "" : "." + col) + RapIndex::kSuffix;
    }

protected:
    struct Result {
        int64_t planned_bytes = 0;
        std::map<uint64_t, int64_t> bytes_by_first_row;
        size_t groups_kept = 0;
        bool file_filtered = false;
        std::vector<std::string> rows; // encode_field() per column (user_id, event_time, model, brand): "N;" or "V<len>:<bytes>;", concatenated; sorted
        struct {
            int rap_index_consulted = 0, rap_index_ready = 0, rap_index_unusable = 0, rap_index_ranges = 0;
            int rap_build_written = 0, rap_build_skipped = 0; // slice 4 (P3b)
        } stats_delta;
    };

    HdfsScannerContext* _ctx(const std::string& path, const std::vector<std::string>& eq_literals) {
        auto* ctx = _pool.add(new HdfsScannerContext());
        auto* counter = _pool.add(new std::atomic<int32_t>(0));
        ctx->format_scan_context.lazy_column_coalesce_counter = counter;
        ctx->format_scan_context.timezone = "Asia/Shanghai";
        ctx->format_scan_context.stats = &g_rap_stats;
        ctx->format_scan_context.options.parquet_page_index_enable = true;
        ctx->format_scan_context.options.parquet_bloom_filter_enable = true;
        ctx->format_scan_context.options.use_file_metacache = _metacache; // slice m2a: registered-state reuse
        ctx->format_scan_context.fs = _scan_fs; // slice 2c: the scan's filesystem for sidecar reads (null = default)
        ctx->format_scan_context.predicate_tree = &ctx->predicates.predicate_tree;
        Utils::SlotDesc slot_descs[] = {
                {"user_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                {"event_time", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
                {"model", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                // astra CX-27: a second string column, so a predicate can target a column the sidecar does not index
                {"brand", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                {""},
        };
        TupleDescriptor* td = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(td, &ctx->format_scan_context.materialized_columns);
        ctx->slot_descs = td->slots();
        auto* range = _pool.add(new THdfsScanRange());
        range->relative_path = path;
        range->file_length = std::filesystem::file_size(path);
        range->offset = 4;
        range->length = range->file_length;
        ctx->scan_range = range;
        ctx->format_scan_context.scan_range_offset = range->offset;
        ctx->format_scan_context.scan_range_length = range->length;
        if (_pred_hook) {
            // slice 4: typed / range / null / multi-slot conjuncts built by the test itself
            _pred_hook(td, ctx);
        } else if (!eq_literals.empty()) {
            // the predicate targets `model` (slot 2, the indexed column) unless a test selects `brand` (slot 3)
            const SlotId pred_slot = td->slots()[_pred_col == "brand" ? 3 : 2]->id();
            std::vector<TExpr> t;
            if (eq_literals.size() == 1) {
                ParquetUTBase::append_string_conjunct(TExprOpcode::EQ, pred_slot, eq_literals[0], &t);
            } else {
                // astra CX-25: IN through the reader, with the helper the test base already has
                std::set<std::string> vals(eq_literals.begin(), eq_literals.end());
                ParquetUTBase::create_in_predicate_string_conjunct_ctxs(TExprOpcode::FILTER_IN, pred_slot, vals, &t);
            }
            ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t, &ctx->format_scan_context.conjunct_ctxs_by_slot[pred_slot]);
            std::vector<ExprContext*> all = ctx->format_scan_context.conjunct_ctxs_by_slot[pred_slot];
            ParquetUTBase::setup_conjuncts_manager(all, nullptr, td, _runtime_state, ctx);
        }
        // slice 4 fix-up 5: the pruner, built the way hdfs_scanner.cpp:182-185 builds it -- a ConnectorPredicateParser
        // over this scan's slots and the conjuncts manager's unarrived-runtime-filter list, which is EMPTY unless the
        // query carries a runtime filter. The pointer is non-null either way; only kOneFilter makes it non-empty.
        if (_rf_mode != RfMode::kNoPruner) {
            auto* parser = _pool.add(new ConnectorPredicateParser(&ctx->slot_descs));
            auto* rf_list = _pool.add(new UnarrivedRuntimeFilterList());
            if (_rf_mode == RfMode::kOneFilter) {
                auto rf = gen_runtime_filter_desc(td->slots()[1]->id()); // event_time, BIGINT
                EXPECT_TRUE(rf.ok()) << rf.status().message();
                if (rf.ok()) rf_list->add_unarrived_rf(rf.value(), td->slots()[1], 0);
            }
            ctx->predicates.runtime_filter_scan_range_pruner = std::make_unique<RuntimeScanRangePruner>(parser, *rf_list);
            ctx->format_scan_context.runtime_filter_scan_range_pruner =
                    ctx->predicates.runtime_filter_scan_range_pruner.get();
        }
        return ctx;
    }

    Result run(const std::string& path, const std::vector<std::string>& literals, const std::string& index_dir,
               std::vector<RowRangeHint> hint = {}) {
        Result out;
        config::rap_index_dir = index_dir;
        const int b_consulted = g_rap_stats.rap_index_consulted, b_ready = g_rap_stats.rap_index_ready,
                  b_unusable = g_rap_stats.rap_index_unusable, b_ranges = g_rap_stats.rap_index_ranges;
        const int b_bw = g_rap_stats.rap_build_written, b_bs = g_rap_stats.rap_build_skipped;
        // pass 1: accounting (never consumed)
        {
            auto* ctx = _ctx(path, literals);
            ctx->format_scan_context.selected_row_ranges = hint;
            auto raw = *FileSystem::Default()->new_random_access_file(path);
            // slice 2g v4: a test may present the local bytes under another storage name (gs://..., s3://...) -- the
            // reader keys the file by the name it is given, exactly as a scan does
            std::unique_ptr<RandomAccessFile> file = _file_name_override.empty()
                                                             ? std::move(raw)
                                                             : std::make_unique<RandomAccessFile>(raw->stream(), _file_name_override);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path), DataCacheOptions(), nullptr, _skip_rows);
            Status st = reader->init(&ctx->format_scan_context);
            EXPECT_TRUE(st.ok()) << st.message();
            const auto& groups = reader->group_readers();
            out.file_filtered = groups.empty();
            out.groups_kept = groups.size();
            for (size_t gi = 0; gi < groups.size(); ++gi) {
                const auto& rg = groups[gi];
                if (rg == nullptr) continue;
                if (gi > 0) EXPECT_TRUE(rg->prepare().ok());
                std::vector<SharedBufferedInputStream::IORange> ranges;
                int64_t end_offset = 0;
                rg->collect_io_ranges(&ranges, &end_offset);
                int64_t g = 0;
                for (const auto& r : ranges) g += r.size;
                out.bytes_by_first_row[rg->get_row_group_first_row()] = g;
                out.planned_bytes += g;
            }
        }
        // pass 2: contents
        {
            auto* ctx = _ctx(path, literals);
            ctx->format_scan_context.selected_row_ranges = hint;
            auto raw = *FileSystem::Default()->new_random_access_file(path);
            // slice 2g v4: a test may present the local bytes under another storage name (gs://..., s3://...) -- the
            // reader keys the file by the name it is given, exactly as a scan does
            std::unique_ptr<RandomAccessFile> file = _file_name_override.empty()
                                                             ? std::move(raw)
                                                             : std::make_unique<RandomAccessFile>(raw->stream(), _file_name_override);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path), DataCacheOptions(), nullptr, _skip_rows);
            Status st = reader->init(&ctx->format_scan_context);
            EXPECT_TRUE(st.ok()) << st.message();
            while (true) {
                auto chunk = std::make_shared<Chunk>();
                chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true), 0);
                chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true), 1);
                chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true), 2);
                chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true), 3);
                Status s2 = reader->get_next(&chunk);
                if (s2.is_end_of_file()) break;
                EXPECT_TRUE(s2.ok()) << s2.message();
                for (size_t r = 0; r < chunk->num_rows(); r++) {
                    std::string row;
                    for (size_t ci = 0; ci < 4; ci++) {
                        const auto& col = chunk->get_column_by_index(ci);
                        if (col->is_null(r)) {
                            row += encode_field(true, "");
                        } else {
                            const Datum d = col->get(r);
                            row += encode_field(false, (ci == 1) ? std::to_string(d.get_int64()) : d.get_slice().to_string());
                        }
                    }
                    out.rows.push_back(std::move(row));
                }
            }
            std::sort(out.rows.begin(), out.rows.end());
        }
        out.stats_delta.rap_index_consulted = g_rap_stats.rap_index_consulted - b_consulted;
        out.stats_delta.rap_index_ready = g_rap_stats.rap_index_ready - b_ready;
        out.stats_delta.rap_index_unusable = g_rap_stats.rap_index_unusable - b_unusable;
        out.stats_delta.rap_index_ranges = g_rap_stats.rap_index_ranges - b_ranges;
        out.stats_delta.rap_build_written = g_rap_stats.rap_build_written - b_bw;
        out.stats_delta.rap_build_skipped = g_rap_stats.rap_build_skipped - b_bs;
        config::rap_index_dir = "";
        return out;
    }

    bool fixtures_present() const {
        return std::filesystem::exists(_fixture_dir + "/" + kFile0) && std::filesystem::exists(_fixture_dir + "/" + kFile1) &&
               std::filesystem::exists(sidecar_of(kFile0)) && std::filesystem::exists(sidecar_of(kFile1));
    }

    // slice 4: conjuncts built by the test -- (slot index, builder(slot id, out)); several slots compose under one AND
    using ConjBuilder = std::function<void(SlotId, std::vector<TExpr>*)>;
    void use_conjuncts(const std::vector<std::pair<int, ConjBuilder>>& conj) {
        _pred_hook = [this, conj](TupleDescriptor* td, HdfsScannerContext* ctx) {
            std::vector<ExprContext*> all;
            for (const auto& [slot_idx, build] : conj) {
                const SlotId sid = td->slots()[slot_idx]->id();
                std::vector<TExpr> t;
                build(sid, &t);
                std::vector<ExprContext*> ctxs;
                ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t, &ctxs);
                auto& by_slot = ctx->format_scan_context.conjunct_ctxs_by_slot[sid];
                by_slot.insert(by_slot.end(), ctxs.begin(), ctxs.end());
                all.insert(all.end(), ctxs.begin(), ctxs.end());
            }
            ParquetUTBase::setup_conjuncts_manager(all, nullptr, td, _runtime_state, ctx);
        };
    }
    void clear_conjuncts() { _pred_hook = nullptr; }

    // slice 4 fix-up 5 (D13's deployed finding). Which runtime-filter pruner this run's scan context carries.
    // kNoPruner is the shape every RAP case used before, and NO production scan has it: HdfsScanner::
    // _build_scanner_context() constructs a RuntimeScanRangePruner unconditionally, so a filter-free query arrives
    // at the reader with an EMPTY pruner, not with none. kEmptyPruner is that real shape; kOneFilter registers one
    // push-downable filter, which is the only shape that may refuse a build.
    enum class RfMode { kNoPruner, kEmptyPruner, kOneFilter };
    void use_runtime_filter_pruner(RfMode m) { _rf_mode = m; }

    // exactly file_reader_test's descriptor: a broadcast TOPN filter targeting one slot of this scan
    StatusOr<RuntimeFilterProbeDescriptor*> gen_runtime_filter_desc(SlotId slot_id) {
        TRuntimeFilterDescription d;
        d.__set_filter_id(1);
        d.__set_has_remote_targets(false);
        d.__set_build_plan_node_id(1);
        d.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
        d.__set_filter_type(TRuntimeFilterBuildType::TOPN_FILTER);
        TExpr col_ref = ExprsTestHelper::create_column_ref_t_expr<TYPE_BIGINT>(slot_id, true);
        d.__isset.plan_node_id_to_target_expr = true;
        d.plan_node_id_to_target_expr.emplace(1, col_ref);
        auto* desc = _pool.add(new RuntimeFilterProbeDescriptor());
        RETURN_IF_ERROR(desc->init(&_pool, d, 1, _runtime_state));
        return desc;
    }

    // slice 4 (P3b): build sidecars for `cols` of `path` with the scan-side builder -- a whole-file, predicate-free read
    Result build_sidecars(const std::string& path, const std::string& dir, const std::string& cols) {
        config::rap_build_index_dir = dir;
        config::rap_build_index_columns = cols;
        const Result r = run(path, {}, "");
        config::rap_build_index_dir = "";
        config::rap_build_index_columns = "";
        return r;
    }

    // slice m2a: one FileReader::init(), returning the consult counters it moved
    struct Consult {
        int consulted = 0, ready = 0, unusable = 0, hit = 0, miss = 0;
        int incompatible = 0; // astra CX-27: a hit whose identity did not match the predicate column
        int negative_hit = 0; // slice 2f: a refused consult answered from the cache
        int64_t load_ns = 0;  // the parse component only
        int64_t consult_ns = 0; // astra CX-28: the WHOLE consult -- key, lookup, any load, postings lookup
        bool ok = false;
    };
    Consult init_once(const std::string& path, const std::string& literal, bool metacache) {
        const bool saved = _metacache;
        _metacache = metacache;
        Consult c;
        const int b_c = g_rap_stats.rap_index_consulted, b_r = g_rap_stats.rap_index_ready, b_u = g_rap_stats.rap_index_unusable,
                  b_h = g_rap_stats.rap_index_cache_hit, b_m = g_rap_stats.rap_index_cache_miss,
                  b_i = g_rap_stats.rap_index_cache_incompatible, b_n = g_rap_stats.rap_index_negative_hit;
        const int64_t b_l = g_rap_stats.rap_index_load_ns, b_cn = g_rap_stats.rap_index_consult_ns;
        {
            auto* ctx = _ctx(path, {literal});
            auto raw = *FileSystem::Default()->new_random_access_file(path);
            // slice 2g v4: a test may present the local bytes under another storage name (gs://..., s3://...) -- the
            // reader keys the file by the name it is given, exactly as a scan does
            std::unique_ptr<RandomAccessFile> file = _file_name_override.empty()
                                                             ? std::move(raw)
                                                             : std::make_unique<RandomAccessFile>(raw->stream(), _file_name_override);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path), DataCacheOptions(), nullptr, _skip_rows);
            c.ok = reader->init(&ctx->format_scan_context).ok();
        }
        c.consulted = g_rap_stats.rap_index_consulted - b_c;
        c.ready = g_rap_stats.rap_index_ready - b_r;
        c.unusable = g_rap_stats.rap_index_unusable - b_u;
        c.hit = g_rap_stats.rap_index_cache_hit - b_h;
        c.miss = g_rap_stats.rap_index_cache_miss - b_m;
        c.incompatible = g_rap_stats.rap_index_cache_incompatible - b_i;
        c.negative_hit = g_rap_stats.rap_index_negative_hit - b_n;
        c.load_ns = g_rap_stats.rap_index_load_ns - b_l;
        c.consult_ns = g_rap_stats.rap_index_consult_ns - b_cn;
        _metacache = saved;
        return c;
    }

    RuntimeState* _runtime_state = nullptr;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
    ObjectPool _pool;
    std::string _fixture_dir, _index_dir, _saved_dir;
    std::string _legacy_index_dir, _mirror, _saved_build_dir, _saved_build_cols; // slice 2g v3 / slice 4
    bool _metacache = false;
    std::string _pred_col = "model"; // astra CX-27: which string column the predicate targets ("model" or "brand")
    FileSystem* _scan_fs = nullptr;  // slice 2c: FileSystem handed to the reader for sidecar reads (null = default)
    std::function<void(TupleDescriptor*, HdfsScannerContext*)> _pred_hook; // slice 4: test-built conjuncts
    SkipRowsContextPtr _skip_rows;                                          // slice 4: a delete filter for the reader
    RfMode _rf_mode = RfMode::kNoPruner; // slice 4 fix-up 5: which pruner shape this run's scan context carries
    std::string _file_name_override;                                        // slice 2g v4: the data file's name as another namespace sees it
};

// 1. The gate, on synthetic bytes: valid loads READY; every corruption / mismatch is UNUSABLE with
//    its reason; and the reason is the FIRST failing check so a control cannot pass for another
//    reason.
TEST_F(RapIndexTest, GateRefusals) {
    RapIndex::Identity id{"f.parquet", 1000, 50000, "model", 15};
    const std::string good = encode("f.parquet", 1000, 50000, "model", 15, 20000,
                                    {{"A", {{0, 20000}}}, {"B", {{20000, 40000}, {40000, 50000}}}});
    auto r = RapIndex::parse(good, id);
    ASSERT_EQ(r.state, RapIndex::State::READY) << r.reason;
    EXPECT_EQ(r.index->num_values(), 2u);
    EXPECT_EQ(r.index->granularity_rows(), 20000u);
    auto rs = r.index->lookup({"B"});
    ASSERT_EQ(rs.size(), 1u) << "adjacent ranges must merge";
    EXPECT_EQ(rs[0].start_row, 20000);
    EXPECT_EQ(rs[0].end_row, 50000);
    rs = r.index->lookup({"A", "B"});
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].start_row, 0);
    EXPECT_EQ(rs[0].end_row, 50000);
    EXPECT_TRUE(r.index->lookup({"ZZZ"}).empty());

    auto expect_unusable = [&](std::string bytes, const RapIndex::Identity& ident, const std::string& why_contains) {
        auto x = RapIndex::parse(bytes, ident);
        EXPECT_EQ(x.state, RapIndex::State::UNUSABLE) << why_contains;
        EXPECT_NE(x.reason.find(why_contains), std::string::npos) << "got reason: " << x.reason;
        EXPECT_EQ(x.index, nullptr);
    };
    std::string b = good;
    b[0] = 'X';
    expect_unusable(b, id, "magic");
    b = good;
    b[b.size() - 1] = 'Y'; // the last byte of "RAPX" is already 'X' -- writing 'X' corrupted nothing (first run)
    expect_unusable(b, id, "magic");
    b = good;
    b[b.size() / 2] ^= 0x01;
    expect_unusable(b, id, "crc32c");
    b = good;
    b[4] = 3; // version (2 is RAPX v2 since slice 4; a v1 body read as v2 fails on postings_offset, not on the version)
    expect_unusable(with_crc(b), id, "version");
    expect_unusable(good, RapIndex::Identity{"other.parquet", 1000, 50000, "model", 15}, "file_name");
    expect_unusable(good, RapIndex::Identity{"f.parquet", 1001, 50000, "model", 15}, "file_size");
    expect_unusable(good, RapIndex::Identity{"f.parquet", 1000, 49999, "model", 15}, "file_rows");
    expect_unusable(good, RapIndex::Identity{"f.parquet", 1000, 50000, "brand", 15}, "column");
    expect_unusable(good, RapIndex::Identity{"f.parquet", 1000, 50000, "model", 16}, "field_id");
    // a schema without field ids (-1) does not refuse on field id
    EXPECT_EQ(RapIndex::parse(good, RapIndex::Identity{"f.parquet", 1000, 50000, "model", -1}).state, RapIndex::State::READY);
    // unsorted values
    const std::string unsorted = encode("f.parquet", 1000, 50000, "model", 15, 20000, {{"B", {{0, 20000}}}, {"A", {{20000, 40000}}}});
    expect_unusable(unsorted, id, "sorted");
    // a range beyond the file
    const std::string beyond = encode("f.parquet", 1000, 50000, "model", 15, 20000, {{"A", {{0, 60000}}}});
    expect_unusable(beyond, id, "beyond");
    // absent path
    EXPECT_EQ(RapIndex::load("/nonexistent/dir/f.parquet.rapx", id).state, RapIndex::State::ABSENT);

    // intersection
    auto x = RapIndex::intersect({{0, 100}, {200, 300}}, {{50, 250}});
    ASSERT_EQ(x.size(), 2u);
    EXPECT_EQ(x[0].start_row, 50);
    EXPECT_EQ(x[0].end_row, 100);
    EXPECT_EQ(x[1].start_row, 200);
    EXPECT_EQ(x[1].end_row, 250);
    EXPECT_TRUE(RapIndex::intersect({{0, 100}}, {{100, 200}}).empty());
}

// 2. The real sidecar loads READY against the real file's identity, and UNUSABLE against a wrong one.
TEST_F(RapIndexTest, RealSidecarIdentity) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string path = _fixture_dir + "/" + kFile0;
    const uint64_t size = std::filesystem::file_size(path);
    auto r = RapIndex::load(sidecar_of(kFile0), RapIndex::Identity{key(kFile0), size, 2576384, "model", 15});
    ASSERT_EQ(r.state, RapIndex::State::READY) << r.reason;
    EXPECT_GT(r.index->num_values(), 3000u);
    EXPECT_EQ(r.index->granularity_rows(), 20000u);
    auto wrong = RapIndex::load(sidecar_of(kFile0), RapIndex::Identity{key(kFile0), size + 1, 2576384, "model", 15});
    EXPECT_EQ(wrong.state, RapIndex::State::UNUSABLE);
    EXPECT_NE(wrong.reason.find("file_size"), std::string::npos) << wrong.reason;
}

// 3. Parity and narrowing through the real reader over the real files.
TEST_F(RapIndexTest, ParityAndNarrowingEQ) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    struct Rung {
        const char* literal;
        bool narrows; // pages hit < pages total in at least one file
    };
    // literals from f2-incremental-n5m.md section 1: L1 hits every page (no narrowing possible),
    // L2 1,100 rows hits 115/129 and 107/122 pages, L3 44 rows and L4 1 row hit few; L5 is absent.
    const Rung rungs[] = {{"V2420", false}, {"25080RABDI", true}, {"2203129G", true}, {"12 Pro", true}, {"__ABSENT_MODEL__", true}};
    for (const std::string file : {kFile0, kFile1}) {
        const std::string path = _fixture_dir + "/" + file;
        for (const auto& rung : rungs) {
            const Result base = run(path, {rung.literal}, "");
            const Result idx = run(path, {rung.literal}, _index_dir);
            SCOPED_TRACE(file + " model = " + rung.literal);
            // parity: same multiset of typed nullable (user_id, event_time, model); sanitized diagnostics
            std::string diag;
            EXPECT_TRUE(same_multiset(idx.rows, base.rows, &diag)) << "indexed read returned different rows: " << diag;
            EXPECT_EQ(idx.stats_delta.rap_index_consulted, 2) << "one consult per pass";
            EXPECT_EQ(idx.stats_delta.rap_index_unusable, 0);
            EXPECT_EQ(base.stats_delta.rap_index_consulted, 0) << "no consult when the dir is unset";
            if (base.rows.empty()) {
                // this file holds no match: the index must filter the file outright
                EXPECT_TRUE(idx.file_filtered);
                EXPECT_EQ(idx.planned_bytes, 0);
            } else if (rung.narrows) {
                EXPECT_LT(idx.planned_bytes, base.planned_bytes) << "index did not narrow planned IO";
                EXPECT_GT(idx.stats_delta.rap_index_ranges, 0);
                for (const auto& [first_row, bytes] : idx.bytes_by_first_row) {
                    auto it = base.bytes_by_first_row.find(first_row);
                    ASSERT_NE(it, base.bytes_by_first_row.end());
                    EXPECT_LT(bytes, it->second);
                }
            } else {
                // every page holds the value: narrowing to all pages must not change planned IO
                EXPECT_EQ(idx.planned_bytes, base.planned_bytes);
            }
        }
    }
}

// 4. Composition: a transport hint disjoint from the index ranges yields zero rows; a hint that
//    covers them yields the indexed result -- intersect, never replace.
TEST_F(RapIndexTest, ComposesWithTransportHint) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string path = _fixture_dir + "/" + kFile0;
    const Result idx = run(path, {"2203129G"}, _index_dir);
    if (idx.rows.empty()) GTEST_SKIP() << "literal absent from this file";
    const Result covering = run(path, {"2203129G"}, _index_dir, {RowRangeHint{0, 3000000}});
    std::string diag;
    EXPECT_TRUE(same_multiset(covering.rows, idx.rows, &diag)) << diag;
    // a hint on rows none of the index ranges touch: pick beyond the file
    const Result disjoint = run(path, {"2203129G"}, _index_dir, {RowRangeHint{2576384, 2576384 + 10}});
    EXPECT_TRUE(disjoint.rows.empty());
    EXPECT_TRUE(disjoint.file_filtered);
}

// 5. astra CX-26: the row encoding is exact -- exercised on the actual helper.
TEST_F(RapIndexTest, RowEncodingIsExact) {
    // null vs the string "NULL"
    EXPECT_NE(encode_field(true, ""), encode_field(false, "NULL"));
    EXPECT_NE(encode_field(true, ""), encode_field(false, ""));
    // embedded separators cannot move a field boundary
    EXPECT_NE(encode_field(false, "a|b") + encode_field(false, "c"), encode_field(false, "a") + encode_field(false, "b|c"));
    EXPECT_NE(encode_field(false, "a;") + encode_field(false, "b"), encode_field(false, "a") + encode_field(false, ";b"));
    EXPECT_NE(encode_field(false, "V1:x;") + encode_field(true, ""), encode_field(false, "V1:x;N;"));
    // the multiset comparison: order-insensitive, multiplicity-sensitive, content-sensitive at equal counts
    std::string d;
    EXPECT_TRUE(same_multiset({"x", "y", "x"}, {"y", "x", "x"}, &d)) << d;
    EXPECT_FALSE(same_multiset({"x", "x", "y"}, {"x", "y", "y"}, &d));
    EXPECT_FALSE(same_multiset({"x", "y"}, {"x", "z"}, &d));
    EXPECT_FALSE(same_multiset({"x"}, {"x", "x"}, &d));
    // diagnostics carry positions and sizes, never contents
    same_multiset({"secret-a"}, {"secret-b"}, &d);
    EXPECT_EQ(d.find("secret"), std::string::npos);
}

// 6. astra CX-25: IN through the reader, with a second literal whose pages the first does not supply.
TEST_F(RapIndexTest, ParityAndNarrowingIN) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string a = "2203129G", b = "220233L2C"; // disjoint page sets in both files (sidecar geometry)
    for (const std::string file : {kFile0, kFile1}) {
        const std::string path = _fixture_dir + "/" + file;
        SCOPED_TRACE(file + " model IN (a, b)");
        const Result base_in = run(path, {a, b}, "");
        const Result idx_in = run(path, {a, b}, _index_dir);
        const Result base_a = run(path, {a}, "");
        const Result base_b = run(path, {b}, "");
        const Result idx_a = run(path, {a}, _index_dir);
        std::string diag;
        // parity of the IN read
        EXPECT_TRUE(same_multiset(idx_in.rows, base_in.rows, &diag)) << "indexed IN differs from unindexed IN: " << diag;
        // the IN result is the multiset union of the two equality results (unindexed truth)
        std::vector<std::string> union_rows = base_a.rows;
        union_rows.insert(union_rows.end(), base_b.rows.begin(), base_b.rows.end());
        EXPECT_TRUE(same_multiset(base_in.rows, union_rows, &diag)) << "IN is not the union of the two EQ reads: " << diag;
        ASSERT_FALSE(base_b.rows.empty()) << "second literal must be present in this file for the test to bite";
        // the second literal contributes rows AND pages the first alone does not supply
        EXPECT_EQ(idx_in.rows.size(), idx_a.rows.size() + base_b.rows.size());
        EXPECT_GT(idx_in.planned_bytes, idx_a.planned_bytes) << "second IN literal added no planned pages";
        EXPECT_LT(idx_in.planned_bytes, base_in.planned_bytes) << "indexed IN did not narrow planned IO";
        EXPECT_EQ(idx_in.stats_delta.rap_index_consulted, 2);
        EXPECT_EQ(idx_in.stats_delta.rap_index_unusable, 0);
    }
}

// 7. astra CX-25: scans -- not just loads -- against an absent, a corrupted and an identity-mismatched
//    sidecar return the complete unindexed result with identical planned IO.
TEST_F(RapIndexTest, ScanWithAbsentOrUnusableSidecar) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string path = _fixture_dir + "/" + kFile1; // L4 ("12 Pro") lives in file 1
    const std::string lit = "2203129G";
    const Result base = run(path, {lit}, "");
    ASSERT_FALSE(base.rows.empty());
    std::string diag;
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_refusal_" + std::to_string(::getpid()));
    fs::create_directories(tmp / "empty");
    fs::create_directories(tmp / "corrupt");
    fs::create_directories(tmp / "mismatch");
    const std::string real = sidecar_of(kFile1);
    std::ifstream in(real, std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);
    const std::string k1 = key(kFile1) + RapIndex::kSuffix; // slice 2g v3: keyed names carry directories
    fs::create_directories((tmp / "corrupt" / k1).parent_path());
    fs::create_directories((tmp / "mismatch" / k1).parent_path());
    {   // crc failure: one payload byte flipped
        std::string c = bytes;
        c[c.size() / 2] ^= 0x01;
        std::ofstream((tmp / "corrupt" / k1).string(), std::ios::binary) << c;
    }
    {   // identity failure: file_size + 1, crc recomputed so ONLY the identity gate can refuse it
        std::string c = bytes;
        uint64_t size = 0;
        std::memcpy(&size, c.data() + 8, 8);
        size += 1;
        std::memcpy(c.data() + 8, &size, 8);
        c = with_crc(c);
        std::ofstream((tmp / "mismatch" / k1).string(), std::ios::binary) << c;
    }
    struct Case {
        const char* name;
        fs::path dir;
        int expect_unusable; // per two passes
    } cases[] = {{"absent", tmp / "empty", 0}, {"corrupt (crc)", tmp / "corrupt", 2}, {"identity mismatch", tmp / "mismatch", 2}};
    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        const Result r = run(path, {lit}, c.dir.string());
        EXPECT_TRUE(same_multiset(r.rows, base.rows, &diag)) << "refused/absent sidecar changed the result: " << diag;
        EXPECT_EQ(r.planned_bytes, base.planned_bytes) << "refused/absent sidecar changed planned IO";
        EXPECT_FALSE(r.file_filtered);
        EXPECT_EQ(r.stats_delta.rap_index_consulted, 2);
        EXPECT_EQ(r.stats_delta.rap_index_ready, 0);
        EXPECT_EQ(r.stats_delta.rap_index_unusable, c.expect_unusable);
        EXPECT_EQ(r.stats_delta.rap_index_ranges, 0);
    }
    fs::remove_all(tmp);
}

// 8. slice m2a: repeated queries reuse identity-bound registered state (slice-m2.md, 2a acceptance).
TEST_F(RapIndexTest, ReusesRegisteredState) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0, f1 = _fixture_dir + "/" + kFile1;
    const std::string lit = "2203129G";
    // a bounded LRU page cache installed the way the reader's own init() finds it
    MemCacheOptions options{.mem_space_size = 100 * 1024 * 1024};
    auto engine = std::make_shared<LRUCacheEngine>();
    ASSERT_TRUE(engine->init(options).ok());
    auto cache = std::make_shared<StoragePageCache>(engine.get());
    auto saved_cache = DataCache::GetInstance()->page_cache_ptr();
    DataCache::GetInstance()->set_page_cache(cache);
    config::rap_index_dir = _index_dir;

    // 1. first read of file 0: miss + load; second read: hit, no load
    const Consult a = init_once(f0, lit, true);
    EXPECT_TRUE(a.ok);
    EXPECT_EQ(a.consulted, 1);
    EXPECT_EQ(a.ready, 1);
    EXPECT_EQ(a.miss, 1);
    EXPECT_EQ(a.hit, 0);
    EXPECT_GT(a.load_ns, 0);
    const Consult b = init_once(f0, lit, true);
    EXPECT_TRUE(b.ok);
    EXPECT_EQ(b.consulted, 1);
    EXPECT_EQ(b.ready, 1);
    EXPECT_EQ(b.hit, 1) << "second read of the same file did not reuse the registered index";
    EXPECT_EQ(b.miss, 0);
    EXPECT_EQ(b.load_ns, 0) << "a cache hit must not parse the sidecar";
    // 2. a different file misses: the state is bound to identity, not to the directory
    const Consult c = init_once(f1, lit, true);
    EXPECT_EQ(c.miss, 1);
    EXPECT_EQ(c.hit, 0);
    EXPECT_GT(c.load_ns, 0);
    // 3. with the metacache option off, no hit ever (milestone-1 path), and the load happens
    const Consult d = init_once(f0, lit, false);
    EXPECT_EQ(d.hit, 0);
    EXPECT_EQ(d.miss, 0);
    EXPECT_GT(d.load_ns, 0);
    EXPECT_EQ(d.ready, 1);
    // 4. rows and planned IO under the cache equal those without it (both indexed) and the unindexed read
    _metacache = true;
    const Result cached = run(f0, {lit}, _index_dir);
    _metacache = false;
    const Result uncached = run(f0, {lit}, _index_dir);
    const Result base = run(f0, {lit}, "");
    std::string diag;
    EXPECT_TRUE(same_multiset(cached.rows, uncached.rows, &diag)) << diag;
    EXPECT_TRUE(same_multiset(cached.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(cached.planned_bytes, uncached.planned_bytes);
    EXPECT_LT(cached.planned_bytes, base.planned_bytes);
    // astra CX-28: a warm consult is not free -- 0 ns PARSE is not 0 ns CONSULT. The whole consult (key,
    // lookup, postings lookup) is timed and must register; its size is reported, not asserted.
    EXPECT_GT(b.consult_ns, 0) << "a warm consult must still be measured";
    EXPECT_GE(a.consult_ns, a.load_ns) << "the consult timer must contain the load it performed";
    // reported, not asserted: cold parse vs. warm reuse (nanoseconds, one execution each); consult totals alongside
    std::cout << "[ REPORT   ] cold sidecar parse " << a.load_ns << " ns; warm reuse parse " << b.load_ns << " ns; different-file parse " << c.load_ns << " ns" << std::endl;
    std::cout << "[ REPORT   ] whole consult: cold " << a.consult_ns << " ns; warm " << b.consult_ns << " ns; different-file " << c.consult_ns
              << " ns; metacache off " << d.consult_ns << " ns" << std::endl;

    DataCache::GetInstance()->set_page_cache(saved_cache);
    config::rap_index_dir = "";
}

// 9. astra CX-27: a cached index is bound to the column it indexes. Warm `model` on file 0, then query
//    `brand` on the same file with a literal that has matching rows (a brand, not a model). The read must
//    return the exact unindexed result, must not filter the file, and must not consume the model postings.
//    Then the hit-time identity check itself: an index object planted under the brand key -- a state the
//    per-column key alone never produces -- is detected as incompatible and not used. Same-column reuse
//    still hits afterwards.
TEST_F(RapIndexTest, CachedIndexRespectsPredicateColumn) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const uint64_t size = std::filesystem::file_size(f0);
    const std::string model_lit = "2203129G";
    const std::string brand_lit = "alps"; // brand = 'alps': rows exist in both fixture files (slice-m2.md, 2a fix); absent from the model index
    MemCacheOptions options{.mem_space_size = 100 * 1024 * 1024};
    auto engine = std::make_shared<LRUCacheEngine>();
    ASSERT_TRUE(engine->init(options).ok());
    auto cache = std::make_shared<StoragePageCache>(engine.get());
    auto saved_cache = DataCache::GetInstance()->page_cache_ptr();
    DataCache::GetInstance()->set_page_cache(cache);
    std::string diag;

    // unindexed truth for the brand predicate (no sidecar dir; the cache option on, as in every read below)
    _pred_col = "brand";
    _metacache = true;
    const Result truth = run(f0, {brand_lit}, "");
    _metacache = false;
    ASSERT_FALSE(truth.rows.empty()) << "the brand literal must match rows in file 0 for the test to bite";
    EXPECT_EQ(truth.stats_delta.rap_index_consulted, 0);

    // 1. warm the cache with the model index
    _pred_col = "model";
    config::rap_index_dir = _index_dir;
    const Consult warm = init_once(f0, model_lit, true);
    EXPECT_TRUE(warm.ok);
    EXPECT_EQ(warm.ready, 1);
    EXPECT_EQ(warm.miss, 1);

    // 2. a brand predicate on the same file with the model index cached: no hit, no postings used, the
    //    loader refuses the column mismatch, and the scan is the complete unindexed one
    _pred_col = "brand";
    const Consult x = init_once(f0, brand_lit, true);
    EXPECT_TRUE(x.ok);
    EXPECT_EQ(x.consulted, 1);
    EXPECT_EQ(x.hit, 0) << "a brand predicate must not hit the model index";
    EXPECT_EQ(x.incompatible, 0) << "a per-column key must not even surface the model object";
    EXPECT_EQ(x.ready, 0);
    EXPECT_EQ(x.unusable, 1) << "the loader must refuse the column mismatch";
    _metacache = true;
    const Result r = run(f0, {brand_lit}, _index_dir);
    _metacache = false;
    EXPECT_TRUE(same_multiset(r.rows, truth.rows, &diag)) << "brand read with a warm model index differs from the unindexed read: " << diag;
    EXPECT_FALSE(r.file_filtered) << "the model postings must not filter a brand predicate's file";
    EXPECT_EQ(r.planned_bytes, truth.planned_bytes);
    EXPECT_EQ(r.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(r.stats_delta.rap_index_ready, 0);
    EXPECT_EQ(r.stats_delta.rap_index_ranges, 0);

    // 3. the identity check itself: plant the model index under the (file 0, brand) key. The reader's
    //    key for a local file in this harness is get_file_cache_key(INDEX, path, mtime 0 -> size) + "|brand".
    config::rap_index_dir = _index_dir;
    {
        auto loaded = RapIndex::load(sidecar_of(kFile0), RapIndex::Identity{key(kFile0), size, 2576384, "model", 15});
        ASSERT_EQ(loaded.state, RapIndex::State::READY) << loaded.reason;
        // slice 2f v2: the planted key is built by the reader's own helper (v1 put the generation into the key and
        // this hand-built string stopped matching -- RAP9 CLEAN 24/25)
        const std::string key = RapIndex::cache_key(false, ParquetUtils::get_file_cache_key(CacheType::INDEX, f0, 0, size), "brand",
                                                    std::string(config::rap_index_generation), "");
        auto* holder = new std::shared_ptr<RapIndex>(std::move(loaded.index));
        auto deleter = [](const starrocks::CacheKey& k, void* v) { delete (std::shared_ptr<RapIndex>*)v; };
        MemCacheWriteOptions wo;
        PageCacheHandle h;
        Status st = cache->insert(key, (void*)holder, static_cast<int64_t>((*holder)->approx_bytes()), deleter, wo, &h);
        ASSERT_TRUE(st.ok()) << st.message();
    }
    const Consult y = init_once(f0, brand_lit, true);
    EXPECT_TRUE(y.ok);
    EXPECT_EQ(y.incompatible, 1) << "the planted model object under the brand key must be detected as incompatible";
    EXPECT_EQ(y.hit, 0) << "an incompatible object is not a hit";
    EXPECT_EQ(y.ready, 0);
    EXPECT_EQ(y.unusable, 1) << "after rejecting the planted object the loader refuses the mismatch and the scan is unindexed";
    _metacache = true;
    const Result r2 = run(f0, {brand_lit}, _index_dir);
    _metacache = false;
    EXPECT_TRUE(same_multiset(r2.rows, truth.rows, &diag)) << "brand read with a planted incompatible object differs from the unindexed read: " << diag;
    EXPECT_FALSE(r2.file_filtered);
    EXPECT_EQ(r2.planned_bytes, truth.planned_bytes);

    // 4. same-column reuse still hits, and the warm consult is measured (astra CX-28)
    _pred_col = "model";
    config::rap_index_dir = _index_dir;
    const Consult z = init_once(f0, model_lit, true);
    EXPECT_TRUE(z.ok);
    EXPECT_EQ(z.hit, 1) << "same-column reuse must still hit";
    EXPECT_EQ(z.incompatible, 0);
    EXPECT_EQ(z.ready, 1);
    EXPECT_EQ(z.load_ns, 0);
    EXPECT_GT(z.consult_ns, 0);
    std::cout << "[ REPORT   ] cross-column consult (refused) " << x.consult_ns << " ns of which parse attempt " << x.load_ns
              << " ns; planted-incompatible consult " << y.consult_ns << " ns; same-column warm consult " << z.consult_ns << " ns" << std::endl;

    DataCache::GetInstance()->set_page_cache(saved_cache);
    config::rap_index_dir = "";
}

// 10. slice 2c: sidecars are read through the scan's FileSystem, and the per-column name is tried first.
//     A memory filesystem stands in for "not the default filesystem": a sidecar that exists ONLY there
//     loads READY through the given fs and is ABSENT through the default one; the reader, handed that
//     fs, consults a per-column-named sidecar in a directory that is EMPTY on the local disk.
TEST_F(RapIndexTest, LoadsSidecarThroughScanFileSystemAndPerColumnName) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const uint64_t size = std::filesystem::file_size(f0);
    const RapIndex::Identity id{key(kFile0), size, 2576384, "model", 15};
    std::ifstream in(sidecar_of(kFile0), std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);

    // 1. the loader reads through the filesystem it is given
    SchemedMemoryFs mem; // slice 2d: schemed, so the reader routes the dir to the scan filesystem
    ASSERT_TRUE(mem.create_dir_recursive("/rap").ok());
    const std::string mp = "/rap/" + kFile0 + RapIndex::kSuffix; // any path: the loader is given it directly
    ASSERT_TRUE(mem.create_file(mp).ok());
    ASSERT_TRUE(mem.append_file(mp, Slice(bytes)).ok());
    auto via_mem = RapIndex::load(&mem, mp, id);
    ASSERT_EQ(via_mem.state, RapIndex::State::READY) << via_mem.reason;
    EXPECT_GT(via_mem.index->num_values(), 3000u);
    EXPECT_EQ(RapIndex::load(mp, id).state, RapIndex::State::ABSENT) << "the default filesystem must not see a memory-only file";
    EXPECT_EQ(RapIndex::load(&mem, "/rap/nothing-here" + std::string(RapIndex::kSuffix), id).state, RapIndex::State::ABSENT);

    // 2. the reader consults through the scan's filesystem, per-column name first. The directory
    //    exists and is EMPTY on the local disk; the sidecar exists only in the memory filesystem,
    //    under <basename>.model.rapx (the legacy name is absent everywhere).
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_2c_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string dir = tmp.string();
    ASSERT_TRUE(mem.create_dir_recursive(dir).ok());
    const std::string percol = mem_path(mem, dir, key(kFile0) + ".model" + RapIndex::kSuffix); // slice 2g v3: keyed
    ASSERT_TRUE(mem.create_file(percol).ok());
    ASSERT_TRUE(mem.append_file(percol, Slice(bytes)).ok());
    std::string diag;
    _scan_fs = &mem;
    const Result base = run(f0, {"2203129G"}, "");
    const Result idx = run(f0, {"2203129G"}, "mem://" + dir); // slice 2d: a schemed dir goes to the scan fs
    _scan_fs = nullptr;
    EXPECT_TRUE(same_multiset(idx.rows, base.rows, &diag)) << "indexed read through the scan fs differs: " << diag;
    ASSERT_FALSE(base.rows.empty());
    EXPECT_EQ(idx.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(idx.stats_delta.rap_index_ready, 2) << "the per-column sidecar in the scan filesystem must load READY";
    EXPECT_EQ(idx.stats_delta.rap_index_unusable, 0);
    EXPECT_GT(idx.stats_delta.rap_index_ranges, 0);
    EXPECT_LT(idx.planned_bytes, base.planned_bytes) << "the index found through the scan fs did not narrow planned IO";

    // 3. control: the same directory through the default filesystem holds nothing -> unindexed, complete
    const Result none = run(f0, {"2203129G"}, dir);
    EXPECT_EQ(none.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(none.stats_delta.rap_index_ready, 0);
    EXPECT_EQ(none.stats_delta.rap_index_unusable, 0);
    EXPECT_TRUE(same_multiset(none.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(none.planned_bytes, base.planned_bytes);
    fs::remove_all(tmp);
}

// 11. slice 2c: the index directory is a runtime-mutable BE config -- the index-off recovery path
//     needs no restart. set_config() must succeed, and the consult must see the new value.
TEST_F(RapIndexTest, IndexDirIsRuntimeMutable) {
    const std::string before = config::rap_index_dir;
    Status st = config::set_config("rap_index_dir", "/rap/runtime-set");
    EXPECT_TRUE(st.ok()) << st.message();
    EXPECT_EQ(std::string(config::rap_index_dir), "/rap/runtime-set");
    st = config::set_config("rap_index_dir", "");
    EXPECT_TRUE(st.ok()) << st.message();
    EXPECT_EQ(std::string(config::rap_index_dir), "");
    config::rap_index_dir = before;
}

// slice 2d (deployed check, attempts 1-2): a remote filesystem's "not found" is ABSENT, not UNUSABLE -- through
// the loader directly and through the reader (a gs:// dir whose objects are missing: consulted, nothing ready,
// nothing unusable, scan complete). A genuine IO error stays UNUSABLE.
TEST_F(RapIndexTest, RemoteNotFoundIsAbsent) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const RapIndex::Identity id{kFile0, std::filesystem::file_size(f0), 2576384, "model", 15};
    RemoteNotFoundFs remote;
    auto r = RapIndex::load(&remote, "gs://bucket/rapx/" + kFile0 + ".model" + RapIndex::kSuffix, id);
    EXPECT_EQ(r.state, RapIndex::State::ABSENT) << r.reason;
    IoErrorFs broken;
    auto u = RapIndex::load(&broken, "gs://bucket/rapx/" + kFile0 + RapIndex::kSuffix, id);
    EXPECT_EQ(u.state, RapIndex::State::UNUSABLE);
    EXPECT_NE(u.reason.find("open:"), std::string::npos) << u.reason;
    std::string diag;
    const Result base = run(f0, {"2203129G"}, "");
    _scan_fs = &remote;
    const Result idx = run(f0, {"2203129G"}, "gs://bucket/rapx");
    _scan_fs = nullptr;
    EXPECT_GE(remote.exists_calls, 4) << "both names must have been asked for on both consults";
    EXPECT_EQ(remote.opens, 0) << "slice 2e: a missing sidecar is never opened";
    EXPECT_EQ(idx.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(idx.stats_delta.rap_index_ready, 0);
    EXPECT_EQ(idx.stats_delta.rap_index_unusable, 0) << "a missing remote sidecar is ABSENT, not unusable";
    EXPECT_TRUE(same_multiset(idx.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(idx.planned_bytes, base.planned_bytes);
}

// slice 2d (deployed check, attempts 1-2): a LOCAL directory is read through the default filesystem even when the
// scan's filesystem is remote; file:// is the same directory; a remote directory still goes through the scan fs.
TEST_F(RapIndexTest, LocalDirIsReadThroughTheDefaultFilesystem) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    std::string diag;
    const Result base = run(f0, {"2203129G"}, "");
    NotFoundFs empty_remote; // stands for the data file's filesystem: it holds no sidecar at all
    _scan_fs = &empty_remote;
    const Result local = run(f0, {"2203129G"}, _index_dir);
    const Result file_uri = run(f0, {"2203129G"}, "file://" + _index_dir);
    EXPECT_EQ(empty_remote.opens, 0) << "a local directory must never be opened through the scan filesystem";
    EXPECT_EQ(empty_remote.exists_calls, 0) << "nor asked for existence through it";
    const Result remote = run(f0, {"2203129G"}, "gs://nowhere/rapx");
    _scan_fs = nullptr;
    EXPECT_GE(empty_remote.exists_calls, 4) << "a remote directory is consulted through the scan filesystem (both names, both consults)";
    EXPECT_EQ(empty_remote.opens, 0) << "slice 2e: a missing sidecar is never opened";
    for (const Result* r : {&local, &file_uri}) {
        EXPECT_EQ(r->stats_delta.rap_index_consulted, 2);
        EXPECT_EQ(r->stats_delta.rap_index_ready, 2) << "a local sidecar dir must load through the default filesystem";
        EXPECT_EQ(r->stats_delta.rap_index_unusable, 0);
        EXPECT_GT(r->stats_delta.rap_index_ranges, 0);
        EXPECT_TRUE(same_multiset(r->rows, base.rows, &diag)) << diag;
        EXPECT_LT(r->planned_bytes, base.planned_bytes);
    }
    EXPECT_EQ(remote.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(remote.stats_delta.rap_index_ready, 0) << "a remote dir is read through the scan fs, which holds nothing";
    EXPECT_EQ(remote.stats_delta.rap_index_unusable, 0);
    EXPECT_TRUE(same_multiset(remote.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(remote.planned_bytes, base.planned_bytes);
}

// slice 2e (deployed check D3-c): the loader asks for existence first, so a missing remote object is ABSENT even
// though the deployed filesystem's open is lazy and its first read fails with a plain IOError.
TEST_F(RapIndexTest, LazyMissingIsAbsentBeforeOpen) {
    const RapIndex::Identity id{kFile0, 57495763, 2576384, "model", 15};
    LazyMissingFs lazy;
    auto r = RapIndex::load(&lazy, "gs://bucket/rapx_percol/" + kFile0 + ".brand" + RapIndex::kSuffix, id);
    EXPECT_EQ(r.state, RapIndex::State::ABSENT) << r.reason;
    EXPECT_NE(r.reason.find("exists: NotFound("), std::string::npos) << r.reason;
    EXPECT_EQ(lazy.exists_calls, 1);
    EXPECT_EQ(lazy.opens, 0) << "a missing sidecar must not be opened (the lazy open would succeed and the read fail)";
}

// the object exists (or vanished after the check): open succeeds lazily, the first thing asked of the stream -- its size,
// which the ceiling needs (slice 4 fix-up 2) -- fails -> UNUSABLE, step named
TEST_F(RapIndexTest, ExistsOkThenReadFailsIsUnusable) {
    const RapIndex::Identity id{kFile0, 57495763, 2576384, "model", 15};
    LazyReadFailFs racy;
    auto u = RapIndex::load(&racy, "gs://bucket/rapx/" + kFile0 + ".model" + RapIndex::kSuffix, id);
    EXPECT_EQ(u.state, RapIndex::State::UNUSABLE);
    EXPECT_EQ(u.reason.rfind("size:", 0), 0u) << u.reason;
    EXPECT_NE(u.reason.find("Fail to get path info"), std::string::npos) << u.reason;
}

// a filesystem that distinguishes: an existence check failing with anything but NotFound is UNUSABLE, not ABSENT
TEST_F(RapIndexTest, ExistsOtherStatusIsUnusable) {
    const RapIndex::Identity id{kFile0, 57495763, 2576384, "model", 15};
    ExistsIoErrorFs denied;
    auto u = RapIndex::load(&denied, "gs://bucket/rapx/" + kFile0 + ".model" + RapIndex::kSuffix, id);
    EXPECT_EQ(u.state, RapIndex::State::UNUSABLE);
    EXPECT_EQ(u.reason.rfind("exists:", 0), 0u) << u.reason;
    EXPECT_EQ(denied.opens, 0);
}

// D3-b's shape: the per-column name is absent, the legacy name holds another column's sidecar -> UNUSABLE
// (column mismatch) through the fallback, never a silent ABSENT
TEST_F(RapIndexTest, LegacyFallbackAfterAbsentIsUnusableColumnMismatch) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    SchemedMemoryFs mem;
    const std::string legacy = encode(key(kFile0), std::filesystem::file_size(f0), 2576384, "brand", 5, 20000, {{"alps", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/rapx").ok());
    const std::string lp = mem_path(mem, "/rapx", key(kFile0) + RapIndex::kSuffix); // slice 2g v3: keyed legacy name
    ASSERT_TRUE(mem.create_file(lp).ok());
    ASSERT_TRUE(mem.append_file(lp, Slice(legacy)).ok());
    std::string diag;
    const Result base = run(f0, {"2203129G"}, "");
    g_rap_stats.rap_index_reason.clear();
    _scan_fs = &mem;
    const Result idx = run(f0, {"2203129G"}, "mem:///rapx");
    _scan_fs = nullptr;
    EXPECT_EQ(idx.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(idx.stats_delta.rap_index_ready, 0);
    EXPECT_EQ(idx.stats_delta.rap_index_unusable, 2) << "the legacy brand sidecar must be refused for the model predicate";
    EXPECT_TRUE(same_multiset(idx.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(idx.planned_bytes, base.planned_bytes);
}

// D3-c's intended shape: both names missing on a lazy remote filesystem -> ABSENT, two existence calls per consult,
// zero opens, results and bytes equal to the ordinary scan
TEST_F(RapIndexTest, BothNamesAbsentOnLazyRemoteIsAbsent) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    std::string diag;
    const Result base = run(f0, {"2203129G"}, "");
    LazyMissingFs lazy;
    g_rap_stats.rap_index_reason.clear();
    _scan_fs = &lazy;
    const Result idx = run(f0, {"2203129G"}, "gs://bucket/rapx_percol");
    _scan_fs = nullptr;
    EXPECT_GE(lazy.exists_calls, 4) << "both names on both consults";
    EXPECT_EQ(lazy.opens, 0) << "never opened: the lazy open would succeed and the read would fail";
    EXPECT_EQ(idx.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(idx.stats_delta.rap_index_ready, 0);
    EXPECT_EQ(idx.stats_delta.rap_index_unusable, 0) << "a missing remote sidecar is ABSENT on the lazy filesystem too";
    EXPECT_TRUE(same_multiset(idx.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(idx.planned_bytes, base.planned_bytes);
}

// the first non-READY outcome reaches the scan stats (the Parquet scanner publishes it as RapIndexConsultReason);
// a READY consult leaves it empty
TEST_F(RapIndexTest, ReasonReachesStats) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    // absent (lazy remote, both names missing)
    LazyMissingFs lazy;
    g_rap_stats.rap_index_reason.clear();
    _scan_fs = &lazy;
    (void)run(f0, {"2203129G"}, "gs://bucket/rapx_percol");
    _scan_fs = nullptr;
    EXPECT_EQ(g_rap_stats.rap_index_reason.rfind("absent: exists: NotFound(", 0), 0u) << g_rap_stats.rap_index_reason;
    // unusable without the fallback: a per-column sidecar whose field id contradicts the schema
    SchemedMemoryFs mem;
    const std::string wrong = encode(key(kFile0), std::filesystem::file_size(f0), 2576384, "model", 99, 20000, {{"2203129G", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/rapx").ok());
    const std::string wp = mem_path(mem, "/rapx", key(kFile0) + ".model" + RapIndex::kSuffix);
    ASSERT_TRUE(mem.create_file(wp).ok());
    ASSERT_TRUE(mem.append_file(wp, Slice(wrong)).ok());
    g_rap_stats.rap_index_reason.clear();
    _scan_fs = &mem;
    (void)run(f0, {"2203129G"}, "mem:///rapx");
    _scan_fs = nullptr;
    EXPECT_EQ(g_rap_stats.rap_index_reason.rfind("unusable: field_id mismatch", 0), 0u) << g_rap_stats.rap_index_reason;
    // ready: the real local sidecars, no reason
    g_rap_stats.rap_index_reason.clear();
    const Result ok = run(f0, {"2203129G"}, _index_dir);
    EXPECT_EQ(ok.stats_delta.rap_index_ready, 2);
    EXPECT_TRUE(g_rap_stats.rap_index_reason.empty()) << g_rap_stats.rap_index_reason;
}

// ---------------------------------------------------------------------------------------------------------------
// slice 2f (slice-2f-negative-cache-and-generation.md): negative caching of refused consults, keyed by generation +
// directory + file identity + column; a generation bump invalidates READY and negative entries alike.
namespace {
struct CacheGuard {
    std::shared_ptr<LRUCacheEngine> engine;
    std::shared_ptr<StoragePageCache> cache;
    std::shared_ptr<StoragePageCache> saved;
    std::string saved_gen;
    CacheGuard() {
        MemCacheOptions options{.mem_space_size = 100 * 1024 * 1024};
        engine = std::make_shared<LRUCacheEngine>();
        EXPECT_TRUE(engine->init(options).ok());
        cache = std::make_shared<StoragePageCache>(engine.get());
        saved = DataCache::GetInstance()->page_cache_ptr();
        DataCache::GetInstance()->set_page_cache(cache);
        saved_gen = config::rap_index_generation;
        config::rap_index_generation = "";
    }
    ~CacheGuard() {
        DataCache::GetInstance()->set_page_cache(saved);
        config::rap_index_generation = saved_gen;
    }
};
} // namespace

// a. the second consult on a directory without the sidecar makes no filesystem call and repeats the outcome + reason
TEST_F(RapIndexTest, NegativeHitSkipsFilesystem) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    LazyMissingFs lazy;
    _scan_fs = &lazy;
    config::rap_index_dir = "gs://bucket/neg_a";
    g_rap_stats.rap_index_reason.clear();
    const Consult c1 = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    const std::string reason1 = g_rap_stats.rap_index_reason;
    EXPECT_EQ(c1.consulted, 1);
    EXPECT_EQ(c1.ready, 0);
    EXPECT_EQ(c1.unusable, 0);
    EXPECT_EQ(c1.negative_hit, 0);
    EXPECT_EQ(lazy.exists_calls, 2) << "both names asked once";
    EXPECT_EQ(reason1.rfind("absent: exists: NotFound(", 0), 0u) << reason1;
    g_rap_stats.rap_index_reason.clear();
    _scan_fs = &lazy;
    const Consult c2 = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    EXPECT_EQ(c2.consulted, 1);
    EXPECT_EQ(c2.negative_hit, 1) << "the refusal must be answered from the cache";
    EXPECT_EQ(c2.ready, 0);
    EXPECT_EQ(c2.unusable, 0);
    EXPECT_EQ(lazy.exists_calls, 2) << "a negative hit makes no filesystem call";
    EXPECT_EQ(lazy.opens, 0);
    EXPECT_EQ(g_rap_stats.rap_index_reason, reason1) << "the cached reason is the first consult's";
}

// b. the negative key includes the DIRECTORY: absence cached for one directory does not answer for another that
//    has the sidecar (D3-e attempt 1 in reverse)
TEST_F(RapIndexTest, NegativeKeyIncludesDirectory) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    LazyMissingFs lazy;
    _scan_fs = &lazy;
    config::rap_index_dir = "gs://bucket/neg_b_empty";
    const Consult c1 = init_once(f0, "2203129G", true);
    EXPECT_EQ(c1.ready, 0);
    (void)init_once(f0, "2203129G", true); // the negative entry now exists for neg_b_empty
    _scan_fs = nullptr;
    SchemedMemoryFs mem;
    const std::string ok_sidecar = encode(key(kFile0), std::filesystem::file_size(f0), 2576384, "model", 15, 20000, {{"2203129G", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/neg_b_full").ok());
    const std::string op = mem_path(mem, "/neg_b_full", key(kFile0) + ".model" + RapIndex::kSuffix);
    ASSERT_TRUE(mem.create_file(op).ok());
    ASSERT_TRUE(mem.append_file(op, Slice(ok_sidecar)).ok());
    _scan_fs = &mem;
    config::rap_index_dir = "mem:///neg_b_full";
    const Consult c3 = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    EXPECT_EQ(c3.negative_hit, 0) << "another directory's absence must not answer for this one";
    EXPECT_EQ(c3.ready, 1) << "the sidecar in this directory must load";
    EXPECT_EQ(c3.miss, 1);
}

// c. a generation bump makes a cached READY index miss and reload
TEST_F(RapIndexTest, GenerationBumpMissesReady) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    config::rap_index_dir = _index_dir;
    const Consult c1 = init_once(f0, "2203129G", true);
    EXPECT_EQ(c1.miss, 1);
    EXPECT_EQ(c1.ready, 1);
    const Consult c2 = init_once(f0, "2203129G", true);
    EXPECT_EQ(c2.hit, 1);
    EXPECT_EQ(c2.miss, 0);
    config::rap_index_generation = "g2";
    const Consult c3 = init_once(f0, "2203129G", true);
    EXPECT_EQ(c3.hit, 0) << "a new generation must not see the old entry";
    EXPECT_EQ(c3.miss, 1);
    EXPECT_EQ(c3.ready, 1);
    EXPECT_GT(c3.load_ns, 0);
}

// d. a generation bump makes a cached negative entry miss: the filesystem is asked again
TEST_F(RapIndexTest, GenerationBumpMissesNegative) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    LazyMissingFs lazy;
    _scan_fs = &lazy;
    config::rap_index_dir = "gs://bucket/neg_d";
    (void)init_once(f0, "2203129G", true);
    EXPECT_EQ(lazy.exists_calls, 2);
    const Consult c2 = init_once(f0, "2203129G", true);
    EXPECT_EQ(c2.negative_hit, 1);
    EXPECT_EQ(lazy.exists_calls, 2);
    config::rap_index_generation = "g2";
    const Consult c3 = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    EXPECT_EQ(c3.negative_hit, 0) << "a new generation must not see the old negative entry";
    EXPECT_EQ(c3.ready, 0);
    EXPECT_EQ(lazy.exists_calls, 4) << "the filesystem is asked again after the bump";
}

// e. an UNUSABLE outcome (legacy sidecar of another column, refused) is cached with its reason: no open on the
//    second consult, still counted unusable, same reason
TEST_F(RapIndexTest, UnusableIsCachedWithReason) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    SchemedMemoryFs mem;
    const std::string legacy = encode(key(kFile0), std::filesystem::file_size(f0), 2576384, "brand", 5, 20000, {{"alps", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/neg_e").ok());
    const std::string lp = mem_path(mem, "/neg_e", key(kFile0) + RapIndex::kSuffix);
    ASSERT_TRUE(mem.create_file(lp).ok());
    ASSERT_TRUE(mem.append_file(lp, Slice(legacy)).ok());
    _scan_fs = &mem;
    config::rap_index_dir = "mem:///neg_e";
    g_rap_stats.rap_index_reason.clear();
    const Consult c1 = init_once(f0, "2203129G", true);
    EXPECT_EQ(c1.unusable, 1);
    EXPECT_EQ(c1.negative_hit, 0);
    const int opens_after_first = mem.opens;
    EXPECT_GE(opens_after_first, 1) << "the legacy sidecar was opened and refused";
    const std::string reason1 = g_rap_stats.rap_index_reason;
    EXPECT_EQ(reason1.rfind("unusable: column mismatch", 0), 0u) << reason1;
    g_rap_stats.rap_index_reason.clear();
    const Consult c2 = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    EXPECT_EQ(c2.negative_hit, 1);
    EXPECT_EQ(c2.unusable, 1) << "a cached refusal still counts as unusable";
    EXPECT_EQ(c2.ready, 0);
    EXPECT_EQ(mem.opens, opens_after_first) << "no open on a negative hit";
    EXPECT_EQ(g_rap_stats.rap_index_reason, reason1);
}

// f. READY entries stay path-independent (slice 2c's contract, D3-e attempt 1's observation, unchanged by 2f): an
//    index loaded from the local directory answers a remote directory that holds nothing
TEST_F(RapIndexTest, ReadyStillPathIndependent) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    config::rap_index_dir = _index_dir;
    const Consult c1 = init_once(f0, "2203129G", true);
    EXPECT_EQ(c1.ready, 1);
    EXPECT_EQ(c1.miss, 1);
    NotFoundFs nothing;
    _scan_fs = &nothing;
    config::rap_index_dir = "gs://nowhere/rapx";
    const Consult c2 = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    EXPECT_EQ(c2.hit, 1) << "READY is keyed by file identity and column, not by directory";
    EXPECT_EQ(c2.ready, 1);
    EXPECT_EQ(c2.negative_hit, 0);
    EXPECT_EQ(nothing.exists_calls, 0) << "served from the cache: no filesystem call";
}

// slice 2f v2 (astra CX-45). g. Key algebra: READY and negative keys live in disjoint namespaces and every field is
//    length-prefixed, so the reviewer's collision (generation "g1|neg|<dir>" vs generation "g1" + directory <dir>), its
//    reverse, and any move of bytes across field boundaries give different keys; equal inputs give equal keys.
TEST_F(RapIndexTest, CacheKeyNamespacesAreDisjoint) {
    const std::string fk = "ix|f.parquet|1|2";
    EXPECT_NE(RapIndex::cache_key(false, fk, "model", "g1|neg|gs://example/rapx", ""),
              RapIndex::cache_key(true, fk, "model", "g1", "gs://example/rapx")) << "the reviewer's collision";
    EXPECT_NE(RapIndex::cache_key(true, fk, "model", "", "gs://x"),
              RapIndex::cache_key(false, fk, "model", "|neg|gs://x", "")) << "the reverse";
    EXPECT_NE(RapIndex::cache_key(false, fk, "mo", "delg", ""), RapIndex::cache_key(false, fk, "model", "g", ""));
    EXPECT_NE(RapIndex::cache_key(true, fk, "model", "g|", "d"), RapIndex::cache_key(true, fk, "model", "g", "|d"));
    EXPECT_NE(RapIndex::cache_key(true, fk, "model", "g", "d"), RapIndex::cache_key(false, fk, "model", "g", "d"));
    EXPECT_EQ(RapIndex::cache_key(false, fk, "model", "g", ""), RapIndex::cache_key(false, fk, "model", "g", "ignored"))
            << "a READY key does not depend on the directory";
    EXPECT_EQ(RapIndex::cache_key(true, fk, "model", "g", "d"), RapIndex::cache_key(true, fk, "model", "g", "d"));
}

// h. Through the real reader: a negative entry stored under generation g1 for a directory is NOT read as a READY index
//    when the generation is the aliasing string -- the READY consult misses, loads from the local directory and is
//    READY, with no negative hit (under the first key scheme this read a NegativeEntry as a shared_ptr<RapIndex>).
TEST_F(RapIndexTest, AliasedGenerationCannotReadNegativeAsReady) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    LazyMissingFs lazy;
    _scan_fs = &lazy;
    config::rap_index_generation = "g1";
    config::rap_index_dir = "gs://bucket/alias_a";
    const Consult neg = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    EXPECT_EQ(neg.ready, 0);
    EXPECT_EQ(lazy.exists_calls, 2);
    config::rap_index_generation = "g1|neg|gs://bucket/alias_a"; // the string that aliased under the first scheme
    config::rap_index_dir = _index_dir;
    const Consult c = init_once(f0, "2203129G", true);
    EXPECT_TRUE(c.ok);
    EXPECT_EQ(c.negative_hit, 0) << "a negative entry must not answer a READY lookup";
    EXPECT_EQ(c.hit, 0);
    EXPECT_EQ(c.miss, 1) << "nothing cached under this key: load";
    EXPECT_EQ(c.ready, 1);
}

// i. The reverse: a READY index cached under the generation "|neg|<dir>" must not answer a negative lookup for <dir>
//    under the empty generation -- the consult pays its existence calls and is ABSENT, no negative hit, no READY.
TEST_F(RapIndexTest, AliasedGenerationCannotReadReadyAsNegative) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    CacheGuard guard;
    const std::string f0 = _fixture_dir + "/" + kFile0;
    config::rap_index_generation = "|neg|gs://bucket/alias_b";
    config::rap_index_dir = _index_dir;
    const Consult r = init_once(f0, "2203129G", true);
    EXPECT_EQ(r.ready, 1);
    EXPECT_EQ(r.miss, 1);
    config::rap_index_generation = "";
    LazyMissingFs lazy;
    _scan_fs = &lazy;
    config::rap_index_dir = "gs://bucket/alias_b";
    const Consult c = init_once(f0, "2203129G", true);
    _scan_fs = nullptr;
    EXPECT_TRUE(c.ok);
    EXPECT_EQ(c.negative_hit, 0) << "a READY entry must not answer a negative lookup";
    EXPECT_EQ(c.ready, 0);
    EXPECT_EQ(c.hit, 0);
    EXPECT_EQ(lazy.exists_calls, 2) << "the consult pays: nothing aliased";
}

// slice 2g (F-COLLISION): the key of a data file is its path under the table's data/ root.
// rekey_sidecar re-encodes a sidecar's header with another file_name (postings copied), CRC and trailer redone.
static std::string rekey_sidecar(const std::string& b, const std::string& new_name) {
    size_t o = 4;
    auto rd32 = [&](size_t at) { uint32_t v; std::memcpy(&v, b.data() + at, 4); return v; };
    auto rd64 = [&](size_t at) { uint64_t v; std::memcpy(&v, b.data() + at, 8); return v; };
    const uint32_t version = rd32(o); o += 4;
    const uint64_t size = rd64(o); o += 8;
    const uint64_t rows = rd64(o); o += 8;
    const uint32_t nlen = rd32(o); o += 4 + nlen;
    const uint32_t clen = rd32(o); o += 4; const std::string col = b.substr(o, clen); o += clen;
    int32_t field_id = 0; std::memcpy(&field_id, b.data() + o, 4); o += 4;
    const uint32_t gran = rd32(o); o += 4;
    const uint32_t nvals = rd32(o); o += 4;
    const uint64_t postings_offset = rd64(o);
    Enc e;
    e.b.append("RAPX", 4); e.put<uint32_t>(version); e.put<uint64_t>(size); e.put<uint64_t>(rows);
    e.bytes(new_name); e.bytes(col); e.put<int32_t>(field_id); e.put<uint32_t>(gran); e.put<uint32_t>(nvals);
    e.put<uint64_t>(e.b.size() + 8);
    e.b.append(b.data() + postings_offset, b.size() - 8 - postings_offset);
    const uint32_t crc = starrocks::crc32c::Value(e.b.data(), e.b.size());
    e.put<uint32_t>(crc); e.b.append("RAPX", 4);
    return e.b;
}

// slice 2g v4 (PRD-01; m37 and m38 reviews): ONE rule -- `<scheme>/` then the full path with its leading slashes
// dropped; a scheme-less path and file:// are the local filesystem, `file/`. The storage namespace, the bucket, the table
// location and the partition directories are all part of the key, so two tables with one suffix cannot share a sidecar,
// a "relative" key cannot coincide with a custom-root key (there are no relative keys any more), and two stores with one
// bucket name cannot share a sidecar (the m38 review's gs:// / s3:// / local collision under v3).
TEST_F(RapIndexTest, KeyOfUnpartitionedPathIsBasename) { // a (v4: the scheme, then the full path)
    EXPECT_EQ(RapIndex::key_of("gs://b/t/data/x.parquet"), "gs/b/t/data/x.parquet");
    EXPECT_EQ(RapIndex::key_of("/root/fixtures/n5m/data/x.parquet"), "file/root/fixtures/n5m/data/x.parquet");
}

TEST_F(RapIndexTest, KeyOfPartitionedPathKeepsPartitionDirs) { // b (v4: and everything before them)
    EXPECT_EQ(RapIndex::key_of("gs://b/t/data/day=1/bucket=2/x.parquet"), "gs/b/t/data/day=1/bucket=2/x.parquet");
    EXPECT_EQ(RapIndex::key_of("gs://b/t/data/user_id_bucket=49/f_10_0_0.parquet"), "gs/b/t/data/user_id_bucket=49/f_10_0_0.parquet");
}

TEST_F(RapIndexTest, KeyUsesLastDataSegment) { // c (v3/v4: no data/ rule; the two review counterexamples)
    EXPECT_EQ(RapIndex::key_of("gs://b/data/t/data/p=data/x.parquet"), "gs/b/data/t/data/p=data/x.parquet");
    // PRD-01: two tables, one suffix under data/
    EXPECT_NE(RapIndex::key_of("gs://fixture-bucket/table-a/data/p=0/part.parquet"),
              RapIndex::key_of("gs://fixture-bucket/table-b/data/p=0/part.parquet"));
    // m37 review: a relative key and a custom-root key shared one namespace under v2
    EXPECT_NE(RapIndex::key_of("gs://b/t/data/b/custom/p=0/x.parquet"), RapIndex::key_of("gs://b/custom/p=0/x.parquet"));
}

TEST_F(RapIndexTest, KeyWithoutDataRootIsTheFullPath) { // d (v2: never the basename; v4: the local filesystem is `file/`)
    EXPECT_EQ(RapIndex::key_of("gs://b/t/other/x.parquet"), "gs/b/t/other/x.parquet");
    EXPECT_EQ(RapIndex::key_of("/root/fixtures/n5m/x.parquet"), "file/root/fixtures/n5m/x.parquet");
    // v4b: a relative local path is resolved against the working directory, so it cannot share `/x.parquet`'s key
    EXPECT_EQ(RapIndex::key_of("x.parquet"), "file" + std::filesystem::current_path().string() + "/x.parquet");
    EXPECT_EQ(RapIndex::key_of("gs://b/t/data/"), "gs/b/t/data/");
}

// slice 2g v4 (m38 review, F-COLLISION / PRD-01). gs://, s3:// and the local filesystem are three namespaces and get three
// keys; file:// IS the local filesystem; a differently spelled scheme (s3a://) is keyed differently -- a miss, never
// another object's postings.
TEST_F(RapIndexTest, KeyKeepsTheStorageScheme) {
    EXPECT_EQ(RapIndex::key_of("gs://fixture-bucket/t/data/p=0/x.parquet"), "gs/fixture-bucket/t/data/p=0/x.parquet");
    EXPECT_EQ(RapIndex::key_of("s3://fixture-bucket/t/data/p=0/x.parquet"), "s3/fixture-bucket/t/data/p=0/x.parquet");
    EXPECT_EQ(RapIndex::key_of("/fixture-bucket/t/data/p=0/x.parquet"), "file/fixture-bucket/t/data/p=0/x.parquet");
    EXPECT_EQ(RapIndex::key_of("file:///fixture-bucket/t/data/p=0/x.parquet"), "file/fixture-bucket/t/data/p=0/x.parquet");
    EXPECT_EQ(RapIndex::key_of("hdfs://nn:8020/w/t/data/x.parquet"), "hdfs/nn:8020/w/t/data/x.parquet");
    EXPECT_EQ(RapIndex::key_of("s3a://b/t/data/x.parquet"), "s3a/b/t/data/x.parquet");
    EXPECT_NE(RapIndex::key_of("gs://fixture-bucket/t/data/p=0/x.parquet"), RapIndex::key_of("s3://fixture-bucket/t/data/p=0/x.parquet"));
    EXPECT_NE(RapIndex::key_of("gs://fixture-bucket/t/data/p=0/x.parquet"), RapIndex::key_of("/fixture-bucket/t/data/p=0/x.parquet"));
}

// e. Two copies of the fixture file with the SAME basename under data/b=0/ and data/b=1/, one sidecar per KEY under
//    the sidecar directory (re-keyed so the stored name is the key), none for data/b=2/: each consult finds its own
//    sidecar (READY, narrowed, rows identical) and the third is ABSENT. Under the basename rule one path would serve
//    all three (or none): the discriminating case for mutant G1.
TEST_F(RapIndexTest, TwoFilesSameBasenameDifferentPartitionsEachReady) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_2g_" + std::to_string(::getpid()));
    for (const char* b : {"b=0", "b=1", "b=2"}) fs::create_directories(tmp / "data" / b);
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string p0 = (tmp / "data" / "b=0" / kFile0).string(), p1 = (tmp / "data" / "b=1" / kFile0).string(),
                      p2 = (tmp / "data" / "b=2" / kFile0).string();
    fs::copy_file(f0, p0); fs::copy_file(f0, p1); fs::copy_file(f0, p2);
    std::ifstream in(sidecar_of(kFile0), std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);
    const std::string dir = (tmp / "rapx").string();
    for (const std::string& p : {p0, p1}) { // slice 2g v3: the key is the file's full path; the sidecar mirrors it
        const fs::path sc = fs::path(dir) / (RapIndex::key_of(p) + ".model" + RapIndex::kSuffix);
        fs::create_directories(sc.parent_path());
        std::ofstream out(sc.string(), std::ios::binary);
        const std::string keyed = rekey_sidecar(bytes, RapIndex::key_of(p));
        out.write(keyed.data(), static_cast<std::streamsize>(keyed.size()));
    }
    std::string diag;
    const Result base = run(p0, {"2203129G"}, "");
    ASSERT_FALSE(base.rows.empty());
    for (const std::string& p : {p0, p1}) {
        const Result idx = run(p, {"2203129G"}, dir);
        EXPECT_TRUE(same_multiset(idx.rows, base.rows, &diag)) << p << ": " << diag;
        EXPECT_EQ(idx.stats_delta.rap_index_consulted, 2) << p;
        EXPECT_EQ(idx.stats_delta.rap_index_ready, 2) << p << ": the sidecar under the file's own partition prefix must load READY";
        EXPECT_EQ(idx.stats_delta.rap_index_unusable, 0) << p;
        EXPECT_LT(idx.planned_bytes, base.planned_bytes) << p;
    }
    const Result none = run(p2, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(none.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(none.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(none.stats_delta.rap_index_ready, 0) << "no sidecar under b=2/: ABSENT, not another partition's";
    EXPECT_EQ(none.stats_delta.rap_index_unusable, 0);
    EXPECT_EQ(none.planned_bytes, base.planned_bytes);
    fs::remove_all(tmp);
}

// slice 2g v2 (m36 review). j: two custom-root paths with one basename get two keys (the basename fallback gave one).
TEST_F(RapIndexTest, FallbackKeysDistinguishCustomRoots) {
    EXPECT_EQ(RapIndex::key_of("gs://b/custom/p=0/x.parquet"), "gs/b/custom/p=0/x.parquet");
    EXPECT_NE(RapIndex::key_of("gs://b/custom/p=0/x.parquet"), RapIndex::key_of("gs://b/custom/p=1/x.parquet"));
}

// k. Two byte-identical copies of the fixture (equal size, equal rows) under custom roots with no data/ segment; a sidecar
//    only for p=0, at its full-path key. p=0 is READY from it; p=1 is ABSENT -- never answered by p=0's sidecar. Under the
//    basename fallback both paths share one key and one sidecar answers both (the m36 review's aliasing).
TEST_F(RapIndexTest, TwoEqualFilesUnderCustomRootsNotAliased) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_2gv2_" + std::to_string(::getpid()));
    for (const char* b : {"p=0", "p=1"}) fs::create_directories(tmp / "custom" / b);
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string p0 = (tmp / "custom" / "p=0" / kFile0).string(), p1 = (tmp / "custom" / "p=1" / kFile0).string();
    fs::copy_file(f0, p0); fs::copy_file(f0, p1);
    ASSERT_EQ(fs::file_size(p0), fs::file_size(p1));
    const std::string key0 = RapIndex::key_of(p0);
    ASSERT_NE(key0, kFile0) << "a path without data/ must not be keyed by its basename";
    ASSERT_NE(key0, RapIndex::key_of(p1));
    std::ifstream in(sidecar_of(kFile0), std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);
    const std::string dir = (tmp / "rapx").string();
    const fs::path sc = fs::path(dir) / (key0 + ".model" + RapIndex::kSuffix);
    fs::create_directories(sc.parent_path());
    { std::ofstream out(sc.string(), std::ios::binary); const std::string keyed = rekey_sidecar(bytes, key0); out.write(keyed.data(), static_cast<std::streamsize>(keyed.size())); }
    std::string diag;
    const Result base = run(p0, {"2203129G"}, "");
    ASSERT_FALSE(base.rows.empty());
    const Result idx0 = run(p0, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(idx0.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(idx0.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(idx0.stats_delta.rap_index_ready, 2) << "p=0 must load its own sidecar under its full-path key";
    EXPECT_LT(idx0.planned_bytes, base.planned_bytes);
    const Result idx1 = run(p1, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(idx1.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(idx1.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(idx1.stats_delta.rap_index_ready, 0) << "p=1 has no sidecar and must not be answered by p=0's";
    EXPECT_EQ(idx1.stats_delta.rap_index_unusable, 0);
    EXPECT_EQ(idx1.planned_bytes, base.planned_bytes);
    fs::remove_all(tmp);
}

// =====================================================================================================================
// slice 2g v3 (PRD-01) + slice 4 (typed keys, ranges, NULL, AND, deletes, scan-side builder) + PRD-02 bounds -- RAP13
// =====================================================================================================================

// v3-x. PRD-01: two byte-identical copies of the fixture under table-a/data/p=0/ and table-b/data/p=0/ (equal size and
//    rows, one suffix); a sidecar only for table-a at its full-path key. table-a is READY from it; table-b is ABSENT --
//    never answered by table-a's postings. Under v2's "path after the last /data/" both files had one key and one
//    sidecar answered both. Mutant G6 (that rule restored) must fail exactly this case and KeyUsesLastDataSegment.
TEST_F(RapIndexTest, TwoTablesSameSuffixNotAliased) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_2gv3_" + std::to_string(::getpid()));
    for (const char* t : {"table-a", "table-b"}) fs::create_directories(tmp / t / "data" / "p=0");
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string pa = (tmp / "table-a" / "data" / "p=0" / kFile0).string(), pb = (tmp / "table-b" / "data" / "p=0" / kFile0).string();
    fs::copy_file(f0, pa); fs::copy_file(f0, pb);
    ASSERT_EQ(fs::file_size(pa), fs::file_size(pb));
    ASSERT_NE(RapIndex::key_of(pa), RapIndex::key_of(pb)) << "two tables with one suffix must have two keys";
    std::ifstream in(sidecar_of(kFile0), std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);
    const std::string dir = (tmp / "rapx").string();
    const fs::path sc = fs::path(dir) / (RapIndex::key_of(pa) + ".model" + RapIndex::kSuffix);
    fs::create_directories(sc.parent_path());
    { std::ofstream out(sc.string(), std::ios::binary); const std::string keyed = rekey_sidecar(bytes, RapIndex::key_of(pa)); out.write(keyed.data(), static_cast<std::streamsize>(keyed.size())); }
    std::string diag;
    const Result base = run(pa, {"2203129G"}, "");
    ASSERT_FALSE(base.rows.empty());
    const Result a = run(pa, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(a.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(a.stats_delta.rap_index_ready, 2) << "table-a must load its own sidecar";
    EXPECT_LT(a.planned_bytes, base.planned_bytes);
    const Result b = run(pb, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(b.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(b.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(b.stats_delta.rap_index_ready, 0) << "table-b has no sidecar and must not be answered by table-a's";
    EXPECT_EQ(b.stats_delta.rap_index_unusable, 0);
    EXPECT_EQ(b.planned_bytes, base.planned_bytes);
    fs::remove_all(tmp);
}

// ---- slice 4 helpers ----------------------------------------------------------------------------------------------
namespace {
int64_t min_int_field(const std::vector<std::string>& rows, size_t field) {
    int64_t m = std::numeric_limits<int64_t>::max();
    for (const auto& r : rows) {
        const auto f = split_fields(r);
        if (field < f.size() && f[field].has_value()) m = std::min<int64_t>(m, std::stoll(*f[field]));
    }
    return m;
}
} // namespace

// slice 4 p/a/b/c/e (INT64 range through the real reader, sidecars built by the SCAN-SIDE builder over the real file):
//    p: a predicate-free whole-file read with rap_build_index_dir set writes one keyed v2 sidecar per listed column;
//    a: `event_time <= min` narrows and returns exactly the unindexed rows; a': `> min` (the complement) has parity;
//    b: `>= min AND <= min` (BETWEEN, inclusive both ends) returns the same rows as a;
//    c: `> min AND < min` (an empty interval) filters the file, and the unindexed scan agrees (no rows);
//    e: `model IS NOT NULL` through the built model sidecar has parity and is READY.
TEST_F(RapIndexTest, TypedRangeParityAndNarrowing) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_range_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string key0 = RapIndex::key_of(f0);
    std::string diag;
    // p: build
    const Result built = build_sidecars(f0, tmp.string(), "event_time,model");
    EXPECT_EQ(built.stats_delta.rap_build_written, 2) << "one sidecar per listed column";
    EXPECT_EQ(built.stats_delta.rap_build_skipped, 0);
    const fs::path et = tmp / (key0 + ".event_time" + RapIndex::kSuffix), md = tmp / (key0 + ".model" + RapIndex::kSuffix);
    ASSERT_TRUE(fs::exists(et)) << et;
    ASSERT_TRUE(fs::exists(md)) << md;
    {
        auto r = RapIndex::load(et.string(), RapIndex::Identity{key0, fs::file_size(f0), 2576384, "event_time", -1});
        ASSERT_EQ(r.state, RapIndex::State::READY) << r.reason;
        EXPECT_EQ(r.index->version(), RapIndex::kVersionV2);
        EXPECT_EQ(r.index->key_type(), RapIndex::KeyType::INT64);
        EXPECT_GT(r.index->num_values(), 0u);
    }
    ASSERT_FALSE(built.rows.empty());
    const int64_t m = min_int_field(built.rows, 1);
    ASSERT_LT(m, std::numeric_limits<int64_t>::max());
    // a
    use_conjuncts({{1, [m](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_bigint_conjunct(TExprOpcode::LE, s, m, t); }}});
    const Result off_le = run(f0, {}, "");
    const Result on_le = run(f0, {}, tmp.string());
    EXPECT_TRUE(same_multiset(on_le.rows, off_le.rows, &diag)) << "a: " << diag;
    EXPECT_GE(off_le.rows.size(), 1u);
    EXPECT_EQ(on_le.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(on_le.stats_delta.rap_index_ready, 2) << "the typed sidecar must answer an INT64 range";
    EXPECT_EQ(on_le.stats_delta.rap_index_unusable, 0);
    EXPECT_LT(on_le.planned_bytes, off_le.planned_bytes) << "a: the minimum lives in few buckets";
    // a'
    use_conjuncts({{1, [m](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_bigint_conjunct(TExprOpcode::GT, s, m, t); }}});
    const Result off_gt = run(f0, {}, "");
    const Result on_gt = run(f0, {}, tmp.string());
    EXPECT_TRUE(same_multiset(on_gt.rows, off_gt.rows, &diag)) << "a': " << diag;
    EXPECT_EQ(on_gt.rows.size() + off_le.rows.size(), built.rows.size()) << "<= min and > min partition the file";
    EXPECT_EQ(on_gt.stats_delta.rap_index_ready, 2);
    // b
    use_conjuncts({{1, [m](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_bigint_conjunct(TExprOpcode::GE, s, m, t); }},
                   {1, [m](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_bigint_conjunct(TExprOpcode::LE, s, m, t); }}});
    const Result off_bt = run(f0, {}, "");
    const Result on_bt = run(f0, {}, tmp.string());
    EXPECT_TRUE(same_multiset(on_bt.rows, off_bt.rows, &diag)) << "b: " << diag;
    EXPECT_TRUE(same_multiset(on_bt.rows, off_le.rows, &diag)) << "b: BETWEEN min AND min == <= min: " << diag;
    EXPECT_EQ(on_bt.stats_delta.rap_index_ready, 2);
    // c
    use_conjuncts({{1, [m](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_bigint_conjunct(TExprOpcode::GT, s, m, t); }},
                   {1, [m](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_bigint_conjunct(TExprOpcode::LT, s, m, t); }}});
    const Result off_empty = run(f0, {}, "");
    const Result on_empty = run(f0, {}, tmp.string());
    EXPECT_TRUE(off_empty.rows.empty());
    EXPECT_TRUE(on_empty.rows.empty());
    EXPECT_TRUE(on_empty.file_filtered) << "c: an empty key interval filters the file";
    // e
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::is_null_pred(s, false, t); }}});
    const Result off_nn = run(f0, {}, "");
    const Result on_nn = run(f0, {}, tmp.string());
    EXPECT_TRUE(same_multiset(on_nn.rows, off_nn.rows, &diag)) << "e: " << diag;
    EXPECT_EQ(on_nn.stats_delta.rap_index_ready, 2) << "IS NOT NULL is answered from the value keys (conservative)";
    EXPECT_FALSE(on_nn.rows.empty());
    // d': `model IS NULL` through the built sidecar -- parity whatever the data holds (the null posting is complete)
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::is_null_pred(s, true, t); }}});
    const Result off_n = run(f0, {}, "");
    const Result on_n = run(f0, {}, tmp.string());
    EXPECT_TRUE(same_multiset(on_n.rows, off_n.rows, &diag)) << "d': " << diag;
    EXPECT_EQ(on_n.stats_delta.rap_index_ready, 2);
    clear_conjuncts();
    fs::remove_all(tmp);
}

// slice 4 d: `IS NULL` is answered from the NULL posting -- a synthetic v2 sidecar with no value keys and one null bucket:
//    exactly one range, planned IO below the ordinary scan, rows equal to the ordinary scan's (which the predicate filters).
//    Mutant T3 (null answered from the value keys) sees no values -> filters the file -> zero ranges.
TEST_F(RapIndexTest, IsNullUsesNullPosting) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_null_" + std::to_string(::getpid()));
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string key0 = RapIndex::key_of(f0);
    const fs::path sc = tmp / (key0 + ".model" + RapIndex::kSuffix);
    fs::create_directories(sc.parent_path());
    const std::string bytes = encode_v2(key0, fs::file_size(f0), 2576384, "model", 15, static_cast<uint8_t>(RapIndex::KeyType::STRING), 20000, {}, {{60000, 80000}});
    { std::ofstream out(sc.string(), std::ios::binary); out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    std::string diag;
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::is_null_pred(s, true, t); }}});
    const Result off = run(f0, {}, "");
    const Result on = run(f0, {}, tmp.string());
    clear_conjuncts();
    EXPECT_EQ(on.stats_delta.rap_index_ready, 2);
    EXPECT_EQ(on.stats_delta.rap_index_ranges, 2) << "one range per pass: the null posting's bucket";
    // the fixture holds no NULL `model`, so the row group's null_count statistic filters the group AFTER the consult ran
    // (the consult sits in init() before the row-group readers): the counters above are the evidence, planned IO is 0
    // on both arms, and the rows agree (none)
    EXPECT_TRUE(same_multiset(on.rows, off.rows, &diag)) << diag;
    for (const auto& r : on.rows) EXPECT_FALSE(split_fields(r)[2].has_value()) << "a non-null model row came back for IS NULL";
    EXPECT_LE(on.rows.size(), off.rows.size());
    fs::remove_all(tmp);
}

// slice 4 f: AND across two indexed columns intersects: `model = X AND event_time <= min` reads no more than the
//    narrower single predicate and returns exactly the ordinary scan's rows. Mutant T4 (union) reads more than
//    `model = X` alone.
TEST_F(RapIndexTest, AndAcrossIndexedColumnsIntersects) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_and_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const Result built = build_sidecars(f0, tmp.string(), "event_time,model");
    ASSERT_EQ(built.stats_delta.rap_build_written, 2);
    const int64_t m = min_int_field(built.rows, 1);
    std::string diag;
    const Result eq_only = run(f0, {"2203129G"}, tmp.string());
    EXPECT_EQ(eq_only.stats_delta.rap_index_ready, 2);
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_string_conjunct(TExprOpcode::EQ, s, "2203129G", t); }},
                   {1, [m](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_bigint_conjunct(TExprOpcode::LE, s, m, t); }}});
    const Result off = run(f0, {}, "");
    const Result on = run(f0, {}, tmp.string());
    clear_conjuncts();
    EXPECT_TRUE(same_multiset(on.rows, off.rows, &diag)) << diag;
    EXPECT_EQ(on.stats_delta.rap_index_consulted, 4) << "two indexed columns, two passes";
    EXPECT_EQ(on.stats_delta.rap_index_ready, 4);
    EXPECT_LE(on.planned_bytes, eq_only.planned_bytes) << "the intersection cannot read more than the EQ alone";

    // slice 4 fix-up 3 (m39): the arm above did NOT discriminate -- mutant T4 (union instead of intersection) survived
    // it, because `event_time <= min` is prunable by the reader's own page index, which clamps both arms to the first
    // event_time page whatever the sidecars say. This arm removes the engine from the comparison: two indexed STRING
    // columns, neither prunable by row-group stats, dictionary filter or page index (D1 on this fixture: `model = …`
    // and `brand = …` each read every row of the file with the index off), so the planned IO is the RAP ranges alone.
    // The two granule sets are DISJOINT -- `model = '2203129G'` occupies 13 of file 0's 20,000-row granules and
    // `brand = 'RCA'` exactly one, with no row in both (measured read-only on ice_poc.poc_lake.pg_n5000000 at S0,
    // 2026-09-13) -- so the intersection is empty and the file is filtered with nothing planned, while a union keeps
    // all 14 granules and plans them. The three sidecar directories (model only, brand only, both) make the claim
    // quantitative: an intersection reads LESS than either column alone, a union at least as much as each.
    const fs::path tmp2 = fs::temp_directory_path() / ("rap_s4_and2_" + std::to_string(::getpid()));
    const std::string both_dir = (tmp2 / "both").string(), model_dir = (tmp2 / "m").string(),
                      brand_dir = (tmp2 / "b").string();
    const Result built2 = build_sidecars(f0, both_dir, "model,brand");
    ASSERT_EQ(built2.stats_delta.rap_build_written, 2);
    const std::string k0 = RapIndex::key_of(f0);
    const std::pair<std::string, std::string> copies[] = {{model_dir, "model"}, {brand_dir, "brand"}};
    for (const auto& [dir, col] : copies) {
        const fs::path dst = fs::path(dir) / (k0 + "." + col + RapIndex::kSuffix);
        fs::create_directories(dst.parent_path());
        fs::copy_file(fs::path(both_dir) / (k0 + "." + col + RapIndex::kSuffix), dst);
    }
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_string_conjunct(TExprOpcode::EQ, s, "2203129G", t); }},
                   {3, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_string_conjunct(TExprOpcode::EQ, s, "RCA", t); }}});
    const Result and_off = run(f0, {}, "");
    const Result and_model = run(f0, {}, model_dir);
    const Result and_brand = run(f0, {}, brand_dir);
    const Result and_both = run(f0, {}, both_dir);
    clear_conjuncts();
    // the case is not vacuous: without the index the file is scanned, and the answer is empty either way
    EXPECT_FALSE(and_off.file_filtered) << "the engine's own pruning must not decide this conjunction by itself";
    EXPECT_GT(and_off.planned_bytes, 0);
    EXPECT_TRUE(and_off.rows.empty()) << "no row of this file carries both values";
    EXPECT_EQ(and_model.stats_delta.rap_index_ready, 2);
    EXPECT_EQ(and_brand.stats_delta.rap_index_ready, 2);
    EXPECT_GT(and_model.planned_bytes, 0);
    EXPECT_GT(and_brand.planned_bytes, 0) << "the brand literal must be present in this file";
    EXPECT_LT(and_model.planned_bytes, and_off.planned_bytes);
    EXPECT_LT(and_brand.planned_bytes, and_off.planned_bytes);
    EXPECT_EQ(and_both.stats_delta.rap_index_consulted, 4);
    EXPECT_EQ(and_both.stats_delta.rap_index_ready, 4);
    EXPECT_TRUE(and_both.rows.empty());
    EXPECT_TRUE(and_both.file_filtered) << "disjoint granule sets intersect to nothing: no row of this file can match";
    EXPECT_EQ(and_both.planned_bytes, 0);
    EXPECT_LT(and_both.planned_bytes, and_model.planned_bytes)
            << "the intersection must read less than either column alone; a union reads at least as much as each";
    EXPECT_LT(and_both.planned_bytes, and_brand.planned_bytes);
    fs::remove_all(tmp2);
    fs::remove_all(tmp);
}

// slice 4 g: a v1 (string-keyed, no null posting) sidecar refuses range and null questions -- UNUSABLE with the reason,
//    the scan complete and equal to the ordinary one -- while EQ on the same sidecar stays READY. Mutant T5 (v1 answers
//    ranges bytewise) reports no refusal.
TEST_F(RapIndexTest, V1RefusesRangeAndNull) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    std::string diag;
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_string_conjunct(TExprOpcode::GE, s, "a", t); }}});
    g_rap_stats.rap_index_reason.clear();
    const Result off_r = run(f0, {}, "");
    const Result on_r = run(f0, {}, _index_dir);
    EXPECT_TRUE(same_multiset(on_r.rows, off_r.rows, &diag)) << diag;
    EXPECT_EQ(on_r.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(on_r.stats_delta.rap_index_ready, 0);
    EXPECT_EQ(on_r.stats_delta.rap_index_unusable, 2) << "a range question to a v1 sidecar must be refused";
    EXPECT_EQ(on_r.planned_bytes, off_r.planned_bytes);
    EXPECT_EQ(g_rap_stats.rap_index_reason.rfind("unusable: range needs v2", 0), 0u) << g_rap_stats.rap_index_reason;
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::is_null_pred(s, true, t); }}});
    g_rap_stats.rap_index_reason.clear();
    const Result off_n = run(f0, {}, "");
    const Result on_n = run(f0, {}, _index_dir);
    EXPECT_TRUE(same_multiset(on_n.rows, off_n.rows, &diag)) << diag;
    EXPECT_EQ(on_n.stats_delta.rap_index_unusable, 2) << "a null question to a v1 sidecar must be refused";
    EXPECT_EQ(g_rap_stats.rap_index_reason.rfind("unusable: null needs v2", 0), 0u) << g_rap_stats.rap_index_reason;
    clear_conjuncts();
    const Result eq = run(f0, {"2203129G"}, _index_dir);
    EXPECT_EQ(eq.stats_delta.rap_index_ready, 2) << "EQ on the same v1 sidecar stays READY";
}

// slice 4 j: a literal whose type cannot be encoded as the sidecar's key type (an INT64-keyed sidecar under a string
//    column's name) refuses the consult: UNUSABLE `literal type`, scan complete and equal.
TEST_F(RapIndexTest, LiteralTypeMismatchRefuses) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_lit_" + std::to_string(::getpid()));
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string key0 = RapIndex::key_of(f0);
    const fs::path sc = tmp / (key0 + ".model" + RapIndex::kSuffix);
    fs::create_directories(sc.parent_path());
    const std::string bytes = encode_v2(key0, fs::file_size(f0), 2576384, "model", 15, static_cast<uint8_t>(RapIndex::KeyType::INT64), 20000,
                                        {{enc_i64(9), {{0, 20000}}}, {enc_i64(10), {{20000, 40000}}}}, {});
    { std::ofstream out(sc.string(), std::ios::binary); out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    std::string diag;
    g_rap_stats.rap_index_reason.clear();
    const Result off = run(f0, {"2203129G"}, "");
    const Result on = run(f0, {"2203129G"}, tmp.string());
    EXPECT_TRUE(same_multiset(on.rows, off.rows, &diag)) << diag;
    EXPECT_EQ(on.stats_delta.rap_index_ready, 0);
    EXPECT_EQ(on.stats_delta.rap_index_unusable, 2);
    EXPECT_EQ(on.planned_bytes, off.planned_bytes);
    EXPECT_EQ(g_rap_stats.rap_index_reason.rfind("unusable: literal type", 0), 0u) << g_rap_stats.rap_index_reason;
    fs::remove_all(tmp);
}

// slice 4 k: a predicate shape the index does not support (`!=`) alone on the indexed column is not consulted at all.
TEST_F(RapIndexTest, UnsupportedShapeNotConsulted) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    std::string diag;
    use_conjuncts({{2, [](SlotId s, std::vector<TExpr>* t) { ParquetUTBase::append_string_conjunct(TExprOpcode::NE, s, "2203129G", t); }}});
    const Result off = run(f0, {}, "");
    const Result on = run(f0, {}, _index_dir);
    clear_conjuncts();
    EXPECT_TRUE(same_multiset(on.rows, off.rows, &diag)) << diag;
    EXPECT_EQ(on.stats_delta.rap_index_consulted, 0) << "!= alone must not consult";
    EXPECT_EQ(on.planned_bytes, off.planned_bytes);
}

// slice 4 m: candidate rows are re-evaluated -- a sidecar whose posting for the literal over-covers the WHOLE file still
//    returns exactly the matching rows. Mutant T7 (rows inside candidate ranges returned unevaluated) returns them all.
TEST_F(RapIndexTest, CandidateRowsAreReEvaluated) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_over_" + std::to_string(::getpid()));
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string key0 = RapIndex::key_of(f0);
    const fs::path sc = tmp / (key0 + ".model" + RapIndex::kSuffix);
    fs::create_directories(sc.parent_path());
    const std::string bytes = encode(key0, fs::file_size(f0), 2576384, "model", 15, 20000, {{"2203129G", {{0, 2576384}}}});
    { std::ofstream out(sc.string(), std::ios::binary); out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    std::string diag;
    const Result off = run(f0, {"2203129G"}, "");
    const Result on = run(f0, {"2203129G"}, tmp.string());
    EXPECT_EQ(on.stats_delta.rap_index_ready, 2);
    EXPECT_TRUE(same_multiset(on.rows, off.rows, &diag)) << "over-covering postings must still yield exactly the matching rows: " << diag;
    EXPECT_FALSE(off.rows.empty());
    fs::remove_all(tmp);
}

// slice 4 l: the delete filter is applied after the index narrowed the read -- every position of the literal's first
//    candidate range deleted; indexed and unindexed reads agree, and both return fewer rows than without the deletes.
//    Mutant T6 (the delete filter skipped when the index is READY) returns the deleted rows on the indexed read.
TEST_F(RapIndexTest, DeleteBitmapStillAppliedAfterIndexNarrowing) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    const std::string f0 = _fixture_dir + "/" + kFile0;
    auto loaded = RapIndex::load(sidecar_of(kFile0), RapIndex::Identity{key(kFile0), std::filesystem::file_size(f0), 2576384, "model", 15});
    ASSERT_EQ(loaded.state, RapIndex::State::READY) << loaded.reason;
    const auto ranges = loaded.index->lookup({"2203129G"});
    ASSERT_FALSE(ranges.empty());
    std::string diag;
    const Result base = run(f0, {"2203129G"}, "");
    ASSERT_FALSE(base.rows.empty());
    roaring64_bitmap_t* bm = roaring64_bitmap_create();
    for (int64_t p = ranges[0].start_row; p < ranges[0].end_row; ++p) roaring64_bitmap_add(bm, static_cast<uint64_t>(p));
    _skip_rows = std::make_shared<SkipRowsContext>();
    _skip_rows->deletion_bitmap = std::make_shared<DeletionBitmap>(bm);
    const Result off = run(f0, {"2203129G"}, "");
    const Result on = run(f0, {"2203129G"}, _index_dir);
    _skip_rows = nullptr;
    EXPECT_EQ(on.stats_delta.rap_index_ready, 2);
    EXPECT_TRUE(same_multiset(on.rows, off.rows, &diag)) << "indexed and unindexed reads under deletes differ: " << diag;
    EXPECT_LT(off.rows.size(), base.rows.size()) << "the deleted range held at least one matching row (it is a posting)";
}

// slice 4 q/r (P3b): the scan-side builder builds nothing under a partial read (a predicate on the file), and never
//    overwrites an existing sidecar (the second build is skipped `exists`, the bytes are unchanged).
TEST_F(RapIndexTest, ScanSideBuilderSkipsPartialReadAndNeverOverwrites) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_build_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string key0 = RapIndex::key_of(f0);
    // q: with a predicate the read is partial -> skipped, nothing written
    config::rap_build_index_dir = tmp.string();
    config::rap_build_index_columns = "model";
    g_rap_stats.rap_build_reason.clear();
    const Result partial = run(f0, {"2203129G"}, "");
    config::rap_build_index_dir = "";
    config::rap_build_index_columns = "";
    EXPECT_EQ(partial.stats_delta.rap_build_written, 0);
    EXPECT_GE(partial.stats_delta.rap_build_skipped, 1);
    // exactly the attach-time refusal: the row-count safety net at finish reports "partial read (rows)" instead (B1)
    EXPECT_EQ(g_rap_stats.rap_build_reason, "partial read");
    EXPECT_FALSE(fs::exists(tmp / (key0 + ".model" + RapIndex::kSuffix)));
    // r: a whole-file read writes it; a second one leaves it alone
    const Result first = build_sidecars(f0, tmp.string(), "model");
    EXPECT_EQ(first.stats_delta.rap_build_written, 1);
    const fs::path sc = tmp / (key0 + ".model" + RapIndex::kSuffix);
    ASSERT_TRUE(fs::exists(sc));
    std::ifstream in1(sc.string(), std::ios::binary);
    const std::string bytes1((std::istreambuf_iterator<char>(in1)), std::istreambuf_iterator<char>());
    g_rap_stats.rap_build_reason.clear();
    const Result second = build_sidecars(f0, tmp.string(), "model");
    EXPECT_EQ(second.stats_delta.rap_build_written, 0);
    EXPECT_GE(second.stats_delta.rap_build_skipped, 1);
    EXPECT_EQ(g_rap_stats.rap_build_reason, "exists");
    std::ifstream in2(sc.string(), std::ios::binary);
    const std::string bytes2((std::istreambuf_iterator<char>(in2)), std::istreambuf_iterator<char>());
    EXPECT_EQ(bytes1, bytes2);
    // s: the scan-built model sidecar agrees with the reference builder's (the v1 fixture): same values, same ranges
    auto scan_built = RapIndex::load(sc.string(), RapIndex::Identity{key0, fs::file_size(f0), 2576384, "model", 15});
    ASSERT_EQ(scan_built.state, RapIndex::State::READY) << scan_built.reason;
    auto reference = RapIndex::load(sidecar_of(kFile0), RapIndex::Identity{key0, fs::file_size(f0), 2576384, "model", 15});
    ASSERT_EQ(reference.state, RapIndex::State::READY) << reference.reason;
    EXPECT_EQ(scan_built.index->num_values(), reference.index->num_values());
    for (const char* v : {"2203129G", "12 Pro", "21061119DG", "V2420"}) {
        const auto a = scan_built.index->lookup({v}), b = reference.index->lookup({v});
        ASSERT_EQ(a.size(), b.size()) << v;
        for (size_t i = 0; i < a.size(); ++i) {
            EXPECT_EQ(a[i].start_row, b[i].start_row) << v;
            EXPECT_EQ(a[i].end_row, b[i].end_row) << v;
        }
    }
    fs::remove_all(tmp);
}

// slice 4 fix-up 5 (D13's FIRST DEPLOYED RUN, 2026-09-13): the scan-side builder must attach on the scan shape the
//    PRODUCTION scanner presents, not the one the unit tests happened to build. Every Hive / Iceberg scan carries a
//    RuntimeScanRangePruner -- hdfs_scanner.cpp:182-185 constructs one unconditionally, EMPTY when the query has no
//    runtime filter -- and the attach test refused on the POINTER being non-null. So the builder never attached on the
//    deployed image: D13's probe of ice_poc.poc_lake.pg_n5000000 recorded `skipped: partial read` on three whole-file,
//    unnarrowed reads (RuntimeFilterNum 0, FilteredRowGroups 0, one row group per file, all 5,000,000 rows read).
//    Every RAP case before this one left the pruner null, which is a shape no production scan has, so 66 green tests
//    and their B1/B2/B3 mutants all passed against a fixture that could not fail this way.
//    t: an EMPTY pruner -- what a filter-free scan really carries -- does NOT refuse the build. One sidecar is written,
//       it loads READY against the file's own identity, and its postings equal the reference builder's value by value.
//    u: a pruner with a push-downable filter REGISTERED does refuse it. The filter can arrive mid-scan and narrow the
//       read through update_range_if_arrived(), so those postings would be short. Nothing is written.
//    Mutant B4 reverts the predicate to `!= nullptr` and must turn t red while leaving u green.
TEST_F(RapIndexTest, ScanSideBuilderAttachesUnderTheEmptyPrunerEveryRealScanCarries) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_rf_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string key0 = RapIndex::key_of(f0);
    const fs::path sc = tmp / (key0 + ".model" + RapIndex::kSuffix);

    // u first, so t cannot pass on a sidecar u left behind: one REGISTERED filter refuses the build
    use_runtime_filter_pruner(RfMode::kOneFilter);
    g_rap_stats.rap_build_reason.clear();
    const Result registered = build_sidecars(f0, tmp.string(), "model");
    EXPECT_EQ(registered.stats_delta.rap_build_written, 0) << "u: a registered runtime filter may narrow the read";
    EXPECT_GE(registered.stats_delta.rap_build_skipped, 1);
    EXPECT_EQ(g_rap_stats.rap_build_reason, "partial read");
    ASSERT_FALSE(fs::exists(sc)) << "u: nothing may be written under a registered filter";

    // t: the same scan with an EMPTY pruner -- the real filter-free shape -- builds
    use_runtime_filter_pruner(RfMode::kEmptyPruner);
    g_rap_stats.rap_build_reason.clear();
    const Result empty_pruner = build_sidecars(f0, tmp.string(), "model");
    EXPECT_EQ(empty_pruner.stats_delta.rap_build_written, 1)
            << "t: an empty pruner is what every filter-free production scan carries; reason=" << g_rap_stats.rap_build_reason;
    EXPECT_EQ(empty_pruner.stats_delta.rap_build_skipped, 0) << "reason=" << g_rap_stats.rap_build_reason;
    ASSERT_TRUE(fs::exists(sc)) << sc;

    // the bytes are a real sidecar, and they are the SAME postings the reference builder produces
    auto built = RapIndex::load(sc.string(), RapIndex::Identity{key0, fs::file_size(f0), 2576384, "model", 15});
    ASSERT_EQ(built.state, RapIndex::State::READY) << built.reason;
    auto reference = RapIndex::load(sidecar_of(kFile0), RapIndex::Identity{key0, fs::file_size(f0), 2576384, "model", 15});
    ASSERT_EQ(reference.state, RapIndex::State::READY) << reference.reason;
    EXPECT_EQ(built.index->num_values(), reference.index->num_values());
    for (const char* v : {"2203129G", "12 Pro", "21061119DG", "V2420"}) {
        const auto a = built.index->lookup({v}), b = reference.index->lookup({v});
        ASSERT_EQ(a.size(), b.size()) << v;
        for (size_t i = 0; i < a.size(); ++i) {
            EXPECT_EQ(a[i].start_row, b[i].start_row) << v;
            EXPECT_EQ(a[i].end_row, b[i].end_row) << v;
        }
    }
    fs::remove_all(tmp);
}

// slice 4 h/i (encoding level, no fixture): DATE / DATETIME / BOOLEAN / INT64 keys order as their types do and a range
//    selects exactly the keys inside the interval; the literal encoder agrees with the key encoder.
TEST_F(RapIndexTest, TypedKeysOrderAndLiteralEncoding) {
    // INT64: 9 < 10 < 100 (bytewise on the canonical encoding; the decimal strings would order 10 < 100 < 9)
    EXPECT_LT(enc_i64(9), enc_i64(10));
    EXPECT_LT(enc_i64(10), enc_i64(100));
    EXPECT_LT(enc_i64(-5), enc_i64(0));
    EXPECT_LT(enc_i64(std::numeric_limits<int64_t>::min()), enc_i64(std::numeric_limits<int64_t>::max()));
    const std::string bytes = encode_v2("k", 1000, 100000, "c", 3, static_cast<uint8_t>(RapIndex::KeyType::INT64), 20000,
                                        {{enc_i64(9), {{0, 20000}}}, {enc_i64(10), {{20000, 40000}}}, {enc_i64(100), {{40000, 60000}}}}, {{80000, 100000}});
    auto r = RapIndex::parse(bytes, RapIndex::Identity{"k", 1000, 100000, "c", 3});
    ASSERT_EQ(r.state, RapIndex::State::READY) << r.reason;
    const std::string k9 = enc_i64(9), k10 = enc_i64(10), k100 = enc_i64(100);
    auto gt9 = r.index->lookup_range(&k9, false, nullptr, true);
    ASSERT_EQ(gt9.size(), 1u);
    EXPECT_EQ(gt9[0].start_row, 20000);
    EXPECT_EQ(gt9[0].end_row, 60000);
    auto between = r.index->lookup_range(&k10, true, &k100, true);
    ASSERT_EQ(between.size(), 1u);
    EXPECT_EQ(between[0].start_row, 20000);
    EXPECT_EQ(between[0].end_row, 60000);
    auto open = r.index->lookup_range(&k10, false, &k100, false);
    EXPECT_TRUE(open.empty()) << "(10, 100) holds no key";
    auto nulls = r.index->null_ranges();
    ASSERT_EQ(nulls.size(), 1u);
    EXPECT_EQ(nulls[0].start_row, 80000);
    // literal encoding through Datum: an INT literal against an INT64 key encodes as the widened value
    std::string lit;
    EXPECT_TRUE(RapIndex::encode_literal(RapIndex::KeyType::INT64, TYPE_INT, Datum(static_cast<int32_t>(10)), &lit));
    EXPECT_EQ(lit, k10);
    EXPECT_TRUE(RapIndex::encode_literal(RapIndex::KeyType::INT64, TYPE_BIGINT, Datum(static_cast<int64_t>(100)), &lit));
    EXPECT_EQ(lit, k100);
    EXPECT_FALSE(RapIndex::encode_literal(RapIndex::KeyType::INT64, TYPE_VARCHAR, Datum(Slice("10")), &lit)) << "a string literal cannot be an INT64 key";
    EXPECT_FALSE(RapIndex::encode_literal(RapIndex::KeyType::STRING, TYPE_INT, Datum(static_cast<int32_t>(1)), &lit));
    // BOOLEAN
    EXPECT_TRUE(RapIndex::encode_literal(RapIndex::KeyType::BOOLEAN, TYPE_BOOLEAN, Datum(static_cast<uint8_t>(1)), &lit));
    EXPECT_EQ(lit, std::string(1, '\x01'));
    // DATE / DATETIME: encoded through the julian day / internal timestamp, which are monotonic in time
    DateValue d1, d2;
    d1.from_date(2026, 9, 12);
    d2.from_date(2026, 9, 13);
    std::string e1, e2;
    EXPECT_TRUE(RapIndex::encode_literal(RapIndex::KeyType::DATE, TYPE_DATE, Datum(d1), &e1));
    EXPECT_TRUE(RapIndex::encode_literal(RapIndex::KeyType::DATE, TYPE_DATE, Datum(d2), &e2));
    EXPECT_LT(e1, e2);
    TimestampValue t1, t2;
    t1.from_timestamp(2026, 9, 12, 23, 59, 59, 0);
    t2.from_timestamp(2026, 9, 13, 0, 0, 0, 0);
    EXPECT_TRUE(RapIndex::encode_literal(RapIndex::KeyType::DATETIME, TYPE_DATETIME, Datum(t1), &e1));
    EXPECT_TRUE(RapIndex::encode_literal(RapIndex::KeyType::DATETIME, TYPE_DATETIME, Datum(t2), &e2));
    EXPECT_LT(e1, e2) << "a day boundary orders as time does";
}

// PRD-02: a CRC-valid header that declares more values / ranges than the body can hold is refused at the count check
//    (before any reserve); a count above the ceiling is refused; an oversize sidecar file is refused before it is read.
TEST_F(RapIndexTest, DeclaredCountsAreBoundedBeforeAllocation) {
    const RapIndex::Identity id{"k", 1000, 100000, "c", 3};
    // a 200-byte body declaring 2^31 - 1 values
    {
        Enc e;
        e.b.append("RAPX", 4);
        e.put<uint32_t>(RapIndex::kVersion); e.put<uint64_t>(1000); e.put<uint64_t>(100000);
        e.bytes("k"); e.bytes("c"); e.put<int32_t>(3); e.put<uint32_t>(20000);
        e.put<uint32_t>(0x7fffffffu);
        e.put<uint64_t>(e.b.size() + 8);
        e.b.append(std::string(200, '\0'));
        const uint32_t crc = starrocks::crc32c::Value(e.b.data(), e.b.size());
        e.put<uint32_t>(crc); e.b.append("RAPX", 4);
        auto r = RapIndex::parse(e.b, id);
        EXPECT_EQ(r.state, RapIndex::State::UNUSABLE);
        EXPECT_NE(r.reason.find("above rap_index_max_values"), std::string::npos) << r.reason;
    }
    // under a ceiling above the count, the body check refuses instead
    {
        const int64_t saved = config::rap_index_max_values;
        config::rap_index_max_values = 1LL << 40;
        Enc e;
        e.b.append("RAPX", 4);
        e.put<uint32_t>(RapIndex::kVersion); e.put<uint64_t>(1000); e.put<uint64_t>(100000);
        e.bytes("k"); e.bytes("c"); e.put<int32_t>(3); e.put<uint32_t>(20000);
        e.put<uint32_t>(1000000u);
        e.put<uint64_t>(e.b.size() + 8);
        e.b.append(std::string(200, '\0'));
        const uint32_t crc = starrocks::crc32c::Value(e.b.data(), e.b.size());
        e.put<uint32_t>(crc); e.b.append("RAPX", 4);
        auto r = RapIndex::parse(e.b, id);
        config::rap_index_max_values = saved;
        EXPECT_EQ(r.state, RapIndex::State::UNUSABLE);
        EXPECT_EQ(r.reason, "declared count exceeds body") << r.reason;
    }
    // one value whose range count exceeds the body
    {
        Enc e;
        e.b.append("RAPX", 4);
        e.put<uint32_t>(RapIndex::kVersion); e.put<uint64_t>(1000); e.put<uint64_t>(100000);
        e.bytes("k"); e.bytes("c"); e.put<int32_t>(3); e.put<uint32_t>(20000);
        e.put<uint32_t>(1u);
        e.put<uint64_t>(e.b.size() + 8);
        e.bytes("v"); e.put<uint32_t>(0x7fffffffu);
        e.b.append(std::string(32, '\0'));
        const uint32_t crc = starrocks::crc32c::Value(e.b.data(), e.b.size());
        e.put<uint32_t>(crc); e.b.append("RAPX", 4);
        auto r = RapIndex::parse(e.b, id);
        EXPECT_EQ(r.state, RapIndex::State::UNUSABLE);
        EXPECT_EQ(r.reason, "declared count exceeds body") << r.reason;
    }
    // the size ceiling applies before the read
    {
        namespace fs = std::filesystem;
        const fs::path tmp = fs::temp_directory_path() / ("rap_prd02_" + std::to_string(::getpid()));
        fs::create_directories(tmp);
        const std::string good = encode("k", 1000, 100000, "c", 3, 20000, {{"v", {{0, 20000}}}});
        const fs::path p = tmp / "k.c.rapx";
        { std::ofstream out(p.string(), std::ios::binary); out.write(good.data(), static_cast<std::streamsize>(good.size())); }
        const int64_t saved = config::rap_index_max_sidecar_bytes;
        config::rap_index_max_sidecar_bytes = 16;
        auto r = RapIndex::load(p.string(), id);
        config::rap_index_max_sidecar_bytes = saved;
        EXPECT_EQ(r.state, RapIndex::State::UNUSABLE);
        EXPECT_EQ(r.reason.rfind("size ", 0), 0u) << r.reason;
        auto ok = RapIndex::load(p.string(), id);
        EXPECT_EQ(ok.state, RapIndex::State::READY) << ok.reason;
        fs::remove_all(tmp);
    }
}

// slice 2g v4b (m39 review, F-COLLISION / PRD-01). A relative local path is not an identity on its own: as v4 was first
// written, `x.parquet` and `/x.parquet` took the same key, so one file's sidecar could be consulted for another file
// entirely. The key now resolves a relative local path against the working directory. Contract first, then the real
// reader: the fixture file is presented under a RELATIVE name, from a working directory that is not its own, with two
// sidecars in the directory -- one keyed by the RESOLVED name (this file's own postings) and one keyed by the
// UNRESOLVED name, which is what an absolute `/<rel>` would use and which here holds the OTHER fixture file's postings.
// The resolved one answers; the unresolved one is never consulted, and on its own it is ABSENT rather than another
// file's index.
TEST_F(RapIndexTest, RelativeLocalNameDoesNotShareAKeyWithAnAbsoluteOne) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    EXPECT_EQ(RapIndex::key_of("/x.parquet"), "file/x.parquet");
    EXPECT_NE(RapIndex::key_of("x.parquet"), RapIndex::key_of("/x.parquet"));
    EXPECT_NE(RapIndex::key_of("a/b/x.parquet"), RapIndex::key_of("/a/b/x.parquet"));
    EXPECT_EQ(RapIndex::key_of("file:///x.parquet"), RapIndex::key_of("/x.parquet"));
    const fs::path tmp = fs::temp_directory_path() / ("rap_rel_" + std::to_string(::getpid()));
    fs::create_directories(tmp / "work");
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string dir = (tmp / "rapx").string();
    const std::string rel = "rap_rel_" + std::to_string(::getpid()) + ".parquet";
    const fs::path cwd0 = fs::current_path();
    fs::current_path(tmp / "work");
    const std::string resolved = RapIndex::key_of(rel);   // file/<tmp>/work/<rel>
    const std::string unresolved = "file/" + rel;         // the key an absolute /<rel> would take
    ASSERT_NE(resolved, unresolved);
    auto place = [&](const std::string& from_file, const std::string& key) {
        std::ifstream in(sidecar_of(from_file), std::ios::binary);
        const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        EXPECT_GT(bytes.size(), 100u);
        const fs::path sc = fs::path(dir) / (key + ".model" + RapIndex::kSuffix);
        fs::create_directories(sc.parent_path());
        std::ofstream out(sc.string(), std::ios::binary);
        const std::string keyed = rekey_sidecar(bytes, key);
        out.write(keyed.data(), static_cast<std::streamsize>(keyed.size()));
        return sc;
    };
    const fs::path a = place(kFile0, resolved);   // this file's own postings, at the resolved key
    place(kFile1, unresolved);                    // the other file's, at the ambiguous key
    std::string diag;
    _file_name_override = rel;
    const Result base = run(f0, {"2203129G"}, "");
    ASSERT_FALSE(base.rows.empty());
    const Result on = run(f0, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(on.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(on.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(on.stats_delta.rap_index_ready, 2) << "the sidecar at the RESOLVED key is this file's";
    EXPECT_EQ(on.stats_delta.rap_index_unusable, 0) << "the sidecar at the unresolved key must never be opened";
    EXPECT_LT(on.planned_bytes, base.planned_bytes);
    fs::remove(a);                                // only the ambiguous sidecar remains
    const Result miss = run(f0, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(miss.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(miss.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(miss.stats_delta.rap_index_ready, 0) << "another path's sidecar must not answer this file";
    EXPECT_EQ(miss.stats_delta.rap_index_unusable, 0) << "it must be ABSENT: the key does not name it at all";
    EXPECT_EQ(miss.planned_bytes, base.planned_bytes);
    _file_name_override.clear();
    fs::current_path(cwd0);
    fs::remove_all(tmp);
}

// slice 4 fix-up 4 (m39, raised by Codex's P3b cancellation question). The scan-side builder writes to the directory it
// attached to, whatever the runtime config says by the time it finishes. Clearing `rap_build_index_dir` mid-scan used to
// leave the finish path with an empty directory, and `sidecar_path("")` is RELATIVE: the BE created directories and wrote
// the sidecar under its own working directory. Here the config is cleared after the first chunk; the sidecar must appear
// in the attached directory and nothing may appear under the process's working directory.
TEST_F(RapIndexTest, ScanSideBuilderUsesTheDirectoryCapturedAtAttach) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_s4_cap_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string rel = RapIndex::key_of(f0) + ".model" + RapIndex::kSuffix;
    const fs::path stray = fs::current_path() / rel; // where an empty directory would send the write
    ASSERT_FALSE(fs::exists(stray));
    config::rap_build_index_dir = tmp.string();
    config::rap_build_index_columns = "model";
    auto* ctx = _ctx(f0, {});
    auto file = *FileSystem::Default()->new_random_access_file(f0);
    auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), fs::file_size(f0),
                                               DataCacheOptions(), nullptr, _skip_rows);
    ASSERT_TRUE(reader->init(&ctx->format_scan_context).ok());
    bool cleared = false;
    while (true) {
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true), 0);
        chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true), 1);
        chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true), 2);
        chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true), 3);
        const Status s = reader->get_next(&chunk);
        if (s.is_end_of_file()) break;
        ASSERT_TRUE(s.ok()) << s.message();
        if (!cleared) { // the operator stops the build while this scan is still reading
            config::rap_build_index_dir = "";
            config::rap_build_index_columns = "";
            cleared = true;
        }
    }
    config::rap_build_index_dir = "";
    config::rap_build_index_columns = "";
    EXPECT_TRUE(cleared) << "the file must have produced at least one chunk before end of file";
    EXPECT_TRUE(fs::exists(tmp / rel)) << "the sidecar belongs in the directory the scan attached to";
    EXPECT_FALSE(fs::exists(stray)) << "a cleared build directory must never make the BE write under its working directory";
    if (fs::exists(stray)) { // only the defect creates these; remove the file and any directories it needed
        std::error_code ec;
        fs::remove(stray, ec);
        for (fs::path p = stray.parent_path(); p != fs::current_path() && !p.empty(); p = p.parent_path()) fs::remove(p, ec);
    }
    fs::remove_all(tmp);
}

// slice 4 fix-up 2 (m38 review, PRD-02). Through a filesystem that cannot report an object's size (fs_hdfs / fs_s3 answer
// NotSupported), the ceiling has to come from the OPENED stream before any payload byte is read or allocated: an oversized
// stream is refused with the size named and ZERO reads; the same object at its true size loads READY through the same
// filesystem (the control); a stream whose size cannot be established is refused, step named.
TEST_F(RapIndexTest, RemoteSizeCeilingRefusesBeforeRead) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    std::ifstream in(sidecar_of(kFile0), std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);
    const uint64_t size = std::filesystem::file_size(_fixture_dir + "/" + kFile0);
    const RapIndex::Identity id{key(kFile0), size, 2576384, "model", 15};
    const std::string url = "gs://bucket/rapx/" + key(kFile0) + RapIndex::kSuffix;
    {
        NoSizeFs big(bytes, config::rap_index_max_sidecar_bytes + 1);
        auto r = RapIndex::load(&big, url, id);
        EXPECT_EQ(r.state, RapIndex::State::UNUSABLE) << r.reason;
        EXPECT_NE(r.reason.find("above rap_index_max_sidecar_bytes"), std::string::npos) << r.reason;
        EXPECT_EQ(big.size_calls, 1) << "the filesystem was asked first (and could not answer)";
        EXPECT_EQ(big.opens, 1);
        EXPECT_EQ(big.stream->reads, 0) << "no payload byte may be read before the reported size is checked";
    }
    {
        NoSizeFs same(bytes, static_cast<int64_t>(bytes.size()));
        auto ok = RapIndex::load(&same, url, id);
        EXPECT_EQ(ok.state, RapIndex::State::READY) << ok.reason;
        EXPECT_GT(same.stream->reads, 0);
        EXPECT_GT(ok.index->num_values(), 3000u);
    }
    {
        NoSizeFs unknown(bytes, -1);
        auto u = RapIndex::load(&unknown, url, id);
        EXPECT_EQ(u.state, RapIndex::State::UNUSABLE) << u.reason;
        EXPECT_EQ(u.reason.rfind("size:", 0), 0u) << u.reason;
        EXPECT_EQ(unknown.stream->reads, 0);
    }
}

// slice 2g v4 (m38 review, F-COLLISION / PRD-01). The SAME bytes seen through three storage namespaces -- gs://, s3://
// and the local filesystem -- with one sidecar in a shared local directory, keyed for the gs:// view. Only the gs:// view
// is READY; the other two are ABSENT (consulted, never answered by another namespace's sidecar), rows equal, bytes equal
// to the unindexed scan. Under v3 the three views had one key and one sidecar answered all of them.
TEST_F(RapIndexTest, TwoNamespacesSameBytesNotAliased) {
    if (!fixtures_present()) GTEST_SKIP() << "fixture files / sidecars not present";
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_2gv4_" + std::to_string(::getpid()));
    fs::create_directories(tmp / "data");
    const std::string f0 = _fixture_dir + "/" + kFile0;
    const std::string local = (tmp / "data" / kFile0).string();
    fs::copy_file(f0, local);
    const std::string rel = local.substr(1); // the path without its leading slash, as a bucket-relative object name
    const std::string via_gs = "gs://" + rel, via_s3 = "s3://" + rel;
    ASSERT_NE(RapIndex::key_of(via_gs), RapIndex::key_of(via_s3));
    ASSERT_NE(RapIndex::key_of(via_gs), RapIndex::key_of(local));
    ASSERT_NE(RapIndex::key_of(via_s3), RapIndex::key_of(local));
    std::ifstream in(sidecar_of(kFile0), std::ios::binary);
    const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);
    const std::string dir = (tmp / "rapx").string();
    const fs::path sc = fs::path(dir) / (RapIndex::key_of(via_gs) + ".model" + RapIndex::kSuffix);
    fs::create_directories(sc.parent_path());
    {
        std::ofstream out(sc.string(), std::ios::binary);
        const std::string keyed = rekey_sidecar(bytes, RapIndex::key_of(via_gs));
        out.write(keyed.data(), static_cast<std::streamsize>(keyed.size()));
    }
    std::string diag;
    const Result base = run(local, {"2203129G"}, "");
    ASSERT_FALSE(base.rows.empty());
    _file_name_override = via_gs;
    const Result gs = run(local, {"2203129G"}, dir);
    EXPECT_TRUE(same_multiset(gs.rows, base.rows, &diag)) << diag;
    EXPECT_EQ(gs.stats_delta.rap_index_consulted, 2);
    EXPECT_EQ(gs.stats_delta.rap_index_ready, 2) << "the gs:// view has its own sidecar";
    EXPECT_EQ(gs.stats_delta.rap_index_unusable, 0);
    EXPECT_LT(gs.planned_bytes, base.planned_bytes);
    for (const std::string& other : {via_s3, std::string()}) { // "" = the local name itself
        _file_name_override = other;
        const Result r = run(local, {"2203129G"}, dir);
        const std::string seen_as = other.empty() ? local : other;
        EXPECT_TRUE(same_multiset(r.rows, base.rows, &diag)) << seen_as << ": " << diag;
        EXPECT_EQ(r.stats_delta.rap_index_consulted, 2) << seen_as;
        EXPECT_EQ(r.stats_delta.rap_index_ready, 0) << seen_as << ": another namespace's sidecar must not answer";
        EXPECT_EQ(r.stats_delta.rap_index_unusable, 0) << seen_as;
        EXPECT_EQ(r.planned_bytes, base.planned_bytes) << seen_as;
    }
    _file_name_override.clear();
    fs::remove_all(tmp);
}

} // namespace starrocks::parquet
