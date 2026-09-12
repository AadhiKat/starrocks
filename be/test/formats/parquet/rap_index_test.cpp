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
#include "fs/fs.h"
#include "base/string/slice.h"
#include "fs/fs_memory.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"

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

std::string with_crc(std::string b) {
    const uint32_t crc = starrocks::crc32c::Value(b.data(), b.size() - 8);
    std::memcpy(b.data() + b.size() - 8, &crc, 4);
    return b;
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

class RapIndexTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
        _fragment_dict_state = std::make_unique<FragmentDictState>();
        _runtime_state->set_fragment_dict_state(_fragment_dict_state.get());
        _fixture_dir = env_or("RAP_FIXTURE_DIR", "/root/fixtures/n5m");
        _index_dir = env_or("RAP_INDEX_DIR", "/root/fixtures/n5m/idx");
        _saved_dir = config::rap_index_dir;
    }
    void TearDown() override { config::rap_index_dir = _saved_dir; }

protected:
    struct Result {
        int64_t planned_bytes = 0;
        std::map<uint64_t, int64_t> bytes_by_first_row;
        size_t groups_kept = 0;
        bool file_filtered = false;
        std::vector<std::string> rows; // encode_field() per column (user_id, event_time, model, brand): "N;" or "V<len>:<bytes>;", concatenated; sorted
        struct {
            int rap_index_consulted = 0, rap_index_ready = 0, rap_index_unusable = 0, rap_index_ranges = 0;
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
        if (!eq_literals.empty()) {
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
        return ctx;
    }

    Result run(const std::string& path, const std::vector<std::string>& literals, const std::string& index_dir,
               std::vector<RowRangeHint> hint = {}) {
        Result out;
        config::rap_index_dir = index_dir;
        const int b_consulted = g_rap_stats.rap_index_consulted, b_ready = g_rap_stats.rap_index_ready,
                  b_unusable = g_rap_stats.rap_index_unusable, b_ranges = g_rap_stats.rap_index_ranges;
        // pass 1: accounting (never consumed)
        {
            auto* ctx = _ctx(path, literals);
            ctx->format_scan_context.selected_row_ranges = hint;
            auto file = *FileSystem::Default()->new_random_access_file(path);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path));
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
            auto file = *FileSystem::Default()->new_random_access_file(path);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path));
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
        config::rap_index_dir = "";
        return out;
    }

    bool fixtures_present() const {
        return std::filesystem::exists(_fixture_dir + "/" + kFile0) && std::filesystem::exists(_fixture_dir + "/" + kFile1) &&
               std::filesystem::exists(_index_dir + "/" + kFile0 + RapIndex::kSuffix) &&
               std::filesystem::exists(_index_dir + "/" + kFile1 + RapIndex::kSuffix);
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
            auto file = *FileSystem::Default()->new_random_access_file(path);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path));
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
    bool _metacache = false;
    std::string _pred_col = "model"; // astra CX-27: which string column the predicate targets ("model" or "brand")
    FileSystem* _scan_fs = nullptr;  // slice 2c: FileSystem handed to the reader for sidecar reads (null = default)
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
    b[4] = 2; // version
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
    auto r = RapIndex::load(_index_dir + "/" + kFile0 + RapIndex::kSuffix, RapIndex::Identity{kFile0, size, 2576384, "model", 15});
    ASSERT_EQ(r.state, RapIndex::State::READY) << r.reason;
    EXPECT_GT(r.index->num_values(), 3000u);
    EXPECT_EQ(r.index->granularity_rows(), 20000u);
    auto wrong = RapIndex::load(_index_dir + "/" + kFile0 + RapIndex::kSuffix, RapIndex::Identity{kFile0, size + 1, 2576384, "model", 15});
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
    const std::string real = _index_dir + "/" + kFile1 + RapIndex::kSuffix;
    std::ifstream in(real, std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);
    {   // crc failure: one payload byte flipped
        std::string c = bytes;
        c[c.size() / 2] ^= 0x01;
        std::ofstream((tmp / "corrupt" / (kFile1 + RapIndex::kSuffix)).string(), std::ios::binary) << c;
    }
    {   // identity failure: file_size + 1, crc recomputed so ONLY the identity gate can refuse it
        std::string c = bytes;
        uint64_t size = 0;
        std::memcpy(&size, c.data() + 8, 8);
        size += 1;
        std::memcpy(c.data() + 8, &size, 8);
        c = with_crc(c);
        std::ofstream((tmp / "mismatch" / (kFile1 + RapIndex::kSuffix)).string(), std::ios::binary) << c;
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
        auto loaded = RapIndex::load(_index_dir + "/" + kFile0 + RapIndex::kSuffix, RapIndex::Identity{kFile0, size, 2576384, "model", 15});
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
    const RapIndex::Identity id{kFile0, size, 2576384, "model", 15};
    std::ifstream in(_index_dir + "/" + kFile0 + RapIndex::kSuffix, std::ios::binary);
    std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_GT(bytes.size(), 100u);

    // 1. the loader reads through the filesystem it is given
    SchemedMemoryFs mem; // slice 2d: schemed, so the reader routes the dir to the scan filesystem
    ASSERT_TRUE(mem.create_dir_recursive("/rap").ok());
    const std::string mem_path = "/rap/" + kFile0 + RapIndex::kSuffix;
    ASSERT_TRUE(mem.create_file(mem_path).ok());
    ASSERT_TRUE(mem.append_file(mem_path, Slice(bytes)).ok());
    auto via_mem = RapIndex::load(&mem, mem_path, id);
    ASSERT_EQ(via_mem.state, RapIndex::State::READY) << via_mem.reason;
    EXPECT_GT(via_mem.index->num_values(), 3000u);
    EXPECT_EQ(RapIndex::load(mem_path, id).state, RapIndex::State::ABSENT) << "the default filesystem must not see a memory-only file";
    EXPECT_EQ(RapIndex::load(&mem, "/rap/nothing-here" + std::string(RapIndex::kSuffix), id).state, RapIndex::State::ABSENT);

    // 2. the reader consults through the scan's filesystem, per-column name first. The directory
    //    exists and is EMPTY on the local disk; the sidecar exists only in the memory filesystem,
    //    under <basename>.model.rapx (the legacy name is absent everywhere).
    namespace fs = std::filesystem;
    const fs::path tmp = fs::temp_directory_path() / ("rap_2c_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    const std::string dir = tmp.string();
    ASSERT_TRUE(mem.create_dir_recursive(dir).ok());
    const std::string percol = dir + "/" + kFile0 + ".model" + RapIndex::kSuffix;
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

// the object exists (or vanished after the check): open succeeds lazily, the read fails -> UNUSABLE, step named
TEST_F(RapIndexTest, ExistsOkThenReadFailsIsUnusable) {
    const RapIndex::Identity id{kFile0, 57495763, 2576384, "model", 15};
    LazyReadFailFs racy;
    auto u = RapIndex::load(&racy, "gs://bucket/rapx/" + kFile0 + ".model" + RapIndex::kSuffix, id);
    EXPECT_EQ(u.state, RapIndex::State::UNUSABLE);
    EXPECT_EQ(u.reason.rfind("read:", 0), 0u) << u.reason;
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
    const std::string legacy = encode(kFile0, std::filesystem::file_size(f0), 2576384, "brand", 5, 20000, {{"alps", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/rapx").ok());
    ASSERT_TRUE(mem.create_file("/rapx/" + kFile0 + RapIndex::kSuffix).ok());
    ASSERT_TRUE(mem.append_file("/rapx/" + kFile0 + RapIndex::kSuffix, Slice(legacy)).ok());
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
    const std::string wrong = encode(kFile0, std::filesystem::file_size(f0), 2576384, "model", 99, 20000, {{"2203129G", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/rapx").ok());
    ASSERT_TRUE(mem.create_file("/rapx/" + kFile0 + ".model" + RapIndex::kSuffix).ok());
    ASSERT_TRUE(mem.append_file("/rapx/" + kFile0 + ".model" + RapIndex::kSuffix, Slice(wrong)).ok());
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
    const std::string ok_sidecar = encode(kFile0, std::filesystem::file_size(f0), 2576384, "model", 15, 20000, {{"2203129G", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/neg_b_full").ok());
    ASSERT_TRUE(mem.create_file("/neg_b_full/" + kFile0 + ".model" + RapIndex::kSuffix).ok());
    ASSERT_TRUE(mem.append_file("/neg_b_full/" + kFile0 + ".model" + RapIndex::kSuffix, Slice(ok_sidecar)).ok());
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
    const std::string legacy = encode(kFile0, std::filesystem::file_size(f0), 2576384, "brand", 5, 20000, {{"alps", {{0, 20000}}}});
    ASSERT_TRUE(mem.create_dir_recursive("/neg_e").ok());
    ASSERT_TRUE(mem.create_file("/neg_e/" + kFile0 + RapIndex::kSuffix).ok());
    ASSERT_TRUE(mem.append_file("/neg_e/" + kFile0 + RapIndex::kSuffix, Slice(legacy)).ok());
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

} // namespace starrocks::parquet
