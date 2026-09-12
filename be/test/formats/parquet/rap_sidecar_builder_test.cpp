// RAP slice 3a -- index at export: the Parquet writer emits a RAPX sidecar for a listed string column
// while it writes the data file. Pre-registered acceptance in slice-3a-export.md section 3.
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <unistd.h>

#include "base/string/slice.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/hive/scanner/hdfs_scanner.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "formats/parquet/rap_index.h"
#include "formats/parquet/rap_sidecar_builder.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"

namespace starrocks::formats {

using starrocks::HdfsScannerContext;
static FormatScannerStats g_stats;

namespace {

// The evaluator upper-cases column 0, so the WRITTEN values differ from the raw input: a builder that
// observed the raw column instead of the evaluated one would index the wrong bytes (mutant Z4).
class UpperEvaluator final : public ColumnEvaluator {
public:
    explicit UpperEvaluator(TypeDescriptor t) : _type(std::move(t)) {}
    Status init() override { return Status::OK(); }
    std::unique_ptr<ColumnEvaluator> clone() const override { return std::make_unique<UpperEvaluator>(_type); }
    TypeDescriptor type() const override { return _type; }
    StatusOr<ColumnPtr> evaluate(Chunk* chunk) override {
        const ColumnPtr& src = chunk->get_column_by_index(0);
        auto out = ColumnHelper::create_column(_type, true);
        for (size_t i = 0; i < src->size(); ++i) {
            if (src->is_null(i)) {
                out->append_nulls(1);
                continue;
            }
            std::string s = src->get(i).get_slice().to_string();
            std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
            out->append_datum(Datum(Slice(s)));
        }
        return out;
    }

private:
    TypeDescriptor _type;
};

// astra CX-36: an evaluator whose SECOND call for the same chunk fails, and which counts its calls. Parquet's
// row-group writer calls it once per chunk; a sidecar builder that evaluated again would either index different
// values (a stateful evaluator) or, as here, fail after the data was written. With slice 3a's memoization the
// builder observes the row-group writer's result and the evaluator runs exactly once per chunk.
class SecondCallFailsEvaluator final : public ColumnEvaluator {
public:
    explicit SecondCallFailsEvaluator(TypeDescriptor t, int* calls, std::set<const Chunk*>* seen)
            : _type(std::move(t)), _calls(calls), _seen(seen) {}
    Status init() override { return Status::OK(); }
    std::unique_ptr<ColumnEvaluator> clone() const override { return std::make_unique<SecondCallFailsEvaluator>(_type, _calls, _seen); }
    TypeDescriptor type() const override { return _type; }
    StatusOr<ColumnPtr> evaluate(Chunk* chunk) override {
        ++*_calls;
        if (!_seen->insert(chunk).second) return Status::InternalError("evaluated twice for the same chunk");
        const ColumnPtr& src = chunk->get_column_by_index(0);
        auto out = ColumnHelper::create_column(_type, true);
        for (size_t i = 0; i < src->size(); ++i) {
            if (src->is_null(i)) {
                out->append_nulls(1);
                continue;
            }
            std::string s = src->get(i).get_slice().to_string();
            std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
            out->append_datum(Datum(Slice(s)));
        }
        return out;
    }

private:
    TypeDescriptor _type;
    int* _calls;
    std::set<const Chunk*>* _seen;
};

// value of row r before upper-casing. Rows [0, 60000) cycle through v10..v49; rows [60000, 100000) cycle through
// ten values inside that range that EXCLUDE v12 (v10, v11, v13..v20). Row 70001 is "v15rare" and row 99999 (the
// last row) is "v17tail" -- the last, partial bucket. Every literal the test queries therefore lies inside every
// page's [min, max] of the written column, so the reader's built-in page zone map cannot prune anything for it and
// the narrowing measured in ConsumerParity is the sidecar's alone (attempt 4 showed the zone map isolating a
// lexical outlier like "RARE" on its own, which left the index nothing to remove).
std::string raw_value(int64_t r) {
    if (r == 70001) return "v15rare";
    if (r == 99999) return "v17tail";
    if (r < 60000) return "v" + std::to_string(10 + (r % 40));
    const int j = static_cast<int>(r % 10);
    return "v" + std::to_string(j < 2 ? 10 + j : 13 + (j - 2));
}
std::string written_value(int64_t r) {
    std::string s = raw_value(r);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::toupper(c); });
    return s;
}
constexpr int64_t kRows = 100000;
constexpr int64_t kChunk = 4000;
constexpr uint32_t G = RapSidecarBuilder::kDefaultGranularityRows;

// expected ranges for a written value, from the formula alone
std::vector<RowRangeHint> expected_ranges(const std::string& wv) {
    std::set<uint32_t> buckets;
    for (int64_t r = 0; r < kRows; ++r) {
        if (written_value(r) == wv) buckets.insert(static_cast<uint32_t>(r / G));
    }
    std::vector<RowRangeHint> out;
    for (uint32_t b : buckets) {
        out.push_back(RowRangeHint{static_cast<int64_t>(b) * G, std::min<int64_t>(static_cast<int64_t>(b + 1) * G, kRows)});
    }
    // merge adjacent, as lookup() does
    std::vector<RowRangeHint> merged;
    for (const auto& r : out) {
        if (!merged.empty() && r.start_row <= merged.back().end_row) merged.back().end_row = std::max(merged.back().end_row, r.end_row);
        else merged.push_back(r);
    }
    return merged;
}

std::string encode_field(bool is_null, const std::string& s) {
    return is_null ? "N;" : "V" + std::to_string(s.size()) + ":" + s + ";";
}

} // namespace

class RapSidecarBuilderTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
        _fragment_dict_state = std::make_unique<FragmentDictState>();
        _runtime_state->set_fragment_dict_state(_fragment_dict_state.get());
        namespace fs = std::filesystem;
        _dir = (fs::temp_directory_path() / ("rap_export_" + std::to_string(::getpid()) + "_" + std::to_string(++_seq))).string();
        fs::create_directories(_dir + "/data");
        fs::create_directories(_dir + "/rapx");
        _saved_dir = config::rap_index_dir;
        config::rap_index_dir = "";
    }
    void TearDown() override {
        config::rap_index_dir = _saved_dir;
        std::filesystem::remove_all(_dir);
    }

protected:
    static std::shared_ptr<FileSystem> default_fs() {
        return std::shared_ptr<FileSystem>(FileSystem::Default(), [](FileSystem*) {});
    }

    // write the 100,000-row file through the real writer; returns (commit result, writer) so stats/rollback are reachable
    struct Written {
        FileCommitResult result;
        std::unique_ptr<ParquetFileWriter> writer;
        std::string path;
    };
    Written write_file(const std::string& rap_dir, std::vector<std::string> rap_columns, bool with_field_ids = true,
                       std::unique_ptr<ColumnEvaluator> k_eval = nullptr) {
        const std::vector<TypeDescriptor> types{TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR),
                                                TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)};
        std::vector<std::unique_ptr<ColumnEvaluator>> evals;
        evals.push_back(k_eval != nullptr ? std::move(k_eval) : std::make_unique<UpperEvaluator>(types[0]));
        auto slot = ColumnSlotIdEvaluator::from_types(types); // slot 1 -> column 1 (v)
        evals.push_back(std::move(slot[1]));
        auto options = std::make_shared<ParquetWriterOptions>();
        // pages smaller than a 20,000-row bucket, so that a bucket-level row range can skip pages: with the default
        // 1 MiB page a dictionary-encoded 100,000-row column is a single page and no narrowing is observable
        options->page_size = 8 * 1024;
        // a 1 KiB dictionary limit keeps `k` (42 short values) dictionary-encoded and forces `v` (100,000 distinct
        // integers) to plain pages, so planned bytes scale with the rows selected instead of with an 800 KB dictionary
        options->dictionary_pagesize = 1024;
        if (with_field_ids) options->column_ids = std::vector<FileColumnId>{FileColumnId{7, {}}, FileColumnId{8, {}}};
        const std::string path = _dir + "/data/export_" + std::to_string(++_seq) + ".parquet";
        auto file = FileSystem::Default()->new_writable_file(path).value();
        auto out = std::make_shared<parquet::ParquetOutputStream>(std::move(file));
        std::string data_path = path;
        auto rollback = [data_path]() { std::filesystem::remove(data_path); };
        auto writer = std::make_unique<ParquetFileWriter>(path, out, std::vector<std::string>{"k", "v"}, types, std::move(evals),
                                                          TCompressionType::NO_COMPRESSION, options, rollback);
        writer->set_rap_export(default_fs(), rap_dir, std::move(rap_columns));
        EXPECT_TRUE(writer->init().ok());
        for (int64_t start = 0; start < kRows; start += kChunk) {
            auto chunk = std::make_shared<Chunk>();
            auto k = ColumnHelper::create_column(types[0], true);
            auto v = ColumnHelper::create_column(types[1], true);
            for (int64_t r = start; r < start + kChunk; ++r) {
                const std::string s = raw_value(r);
                k->append_datum(Datum(Slice(s)));
                v->append_datum(Datum(r));
            }
            chunk->append_column(std::move(k), 0);
            chunk->append_column(std::move(v), 1);
            EXPECT_TRUE(writer->write(chunk.get()).ok());
        }
        Written w;
        w.result = writer->close();
        w.writer = std::move(writer);
        w.path = path;
        return w;
    }

    static std::string basename(const std::string& p) { return p.substr(p.find_last_of('/') + 1); }

    // the real reader over the written file with k = literal; rows encoded exactly, planned IO, consult counters
    struct Read {
        std::vector<std::string> rows;
        int64_t planned_bytes = 0;
        bool file_filtered = false;
        int consulted = 0, ready = 0, unusable = 0, ranges = 0;
    };
    HdfsScannerContext* ctx(const std::string& path, const std::string& literal) {
        auto* c = _pool.add(new HdfsScannerContext());
        c->format_scan_context.lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
        c->format_scan_context.timezone = "Asia/Shanghai";
        c->format_scan_context.stats = &g_stats;
        c->format_scan_context.options.parquet_page_index_enable = true;
        c->format_scan_context.predicate_tree = &c->predicates.predicate_tree;
        parquet::Utils::SlotDesc slots[] = {{"k", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                                            {"v", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)}, {""}};
        TupleDescriptor* td = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slots);
        parquet::Utils::make_column_info_vector(td, &c->format_scan_context.materialized_columns);
        c->slot_descs = td->slots();
        auto* range = _pool.add(new THdfsScanRange());
        range->relative_path = path;
        range->file_length = std::filesystem::file_size(path);
        range->offset = 4;
        range->length = range->file_length;
        c->scan_range = range;
        c->format_scan_context.scan_range_offset = range->offset;
        c->format_scan_context.scan_range_length = range->length;
        const SlotId sid = td->slots()[0]->id();
        std::vector<TExpr> t;
        parquet::ParquetUTBase::append_string_conjunct(TExprOpcode::EQ, sid, literal, &t);
        parquet::ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t, &c->format_scan_context.conjunct_ctxs_by_slot[sid]);
        std::vector<ExprContext*> all = c->format_scan_context.conjunct_ctxs_by_slot[sid];
        parquet::ParquetUTBase::setup_conjuncts_manager(all, nullptr, td, _runtime_state, c);
        return c;
    }
    Read read(const std::string& path, const std::string& literal, const std::string& index_dir) {
        Read out;
        config::rap_index_dir = index_dir;
        const int b_c = g_stats.rap_index_consulted, b_r = g_stats.rap_index_ready, b_u = g_stats.rap_index_unusable, b_g = g_stats.rap_index_ranges;
        {
            auto* c = ctx(path, literal);
            auto file = *FileSystem::Default()->new_random_access_file(path);
            auto reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path));
            EXPECT_TRUE(reader->init(&c->format_scan_context).ok());
            const auto& groups = reader->group_readers();
            out.file_filtered = groups.empty();
            for (size_t gi = 0; gi < groups.size(); ++gi) {
                if (gi > 0) EXPECT_TRUE(groups[gi]->prepare().ok());
                std::vector<SharedBufferedInputStream::IORange> ranges;
                int64_t end = 0;
                groups[gi]->collect_io_ranges(&ranges, &end);
                for (const auto& r : ranges) out.planned_bytes += r.size;
            }
        }
        {
            auto* c = ctx(path, literal);
            auto file = *FileSystem::Default()->new_random_access_file(path);
            auto reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(path));
            EXPECT_TRUE(reader->init(&c->format_scan_context).ok());
            while (true) {
                auto chunk = std::make_shared<Chunk>();
                chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true), 0);
                chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true), 1);
                Status s = reader->get_next(&chunk);
                if (s.is_end_of_file()) break;
                EXPECT_TRUE(s.ok()) << s.message();
                for (size_t r = 0; r < chunk->num_rows(); ++r) {
                    std::string row;
                    for (size_t ci = 0; ci < 2; ++ci) {
                        const auto& col = chunk->get_column_by_index(ci);
                        row += col->is_null(r) ? encode_field(true, "")
                                               : encode_field(false, ci == 1 ? std::to_string(col->get(r).get_int64()) : col->get(r).get_slice().to_string());
                    }
                    out.rows.push_back(row);
                }
            }
            std::sort(out.rows.begin(), out.rows.end());
        }
        out.consulted = g_stats.rap_index_consulted - b_c;
        out.ready = g_stats.rap_index_ready - b_r;
        out.unusable = g_stats.rap_index_unusable - b_u;
        out.ranges = g_stats.rap_index_ranges - b_g;
        config::rap_index_dir = "";
        return out;
    }

    RuntimeState* _runtime_state = nullptr;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
    ObjectPool _pool;
    std::string _dir, _saved_dir;
    static int _seq;
};
int RapSidecarBuilderTest::_seq = 0;

// 1. Round trip: the sidecar written at close loads READY against the file's own identity and its
//    postings equal the formula's, value by value, including the last partial bucket and the rare row.
TEST_F(RapSidecarBuilderTest, RoundTripAndLookup) {
    Written w = write_file(_dir + "/rapx", {"k"});
    ASSERT_TRUE(w.result.io_status.ok()) << w.result.io_status.message();
    EXPECT_EQ(w.result.file_statistics.record_count, kRows);
    EXPECT_EQ(w.writer->rap_export_stats().sidecars_written, 1);
    EXPECT_EQ(w.writer->rap_export_stats().failures, 0);
    EXPECT_GT(w.writer->rap_export_stats().sidecar_bytes, 100);
    const std::string base = basename(w.path);
    const std::string sidecar = _dir + "/rapx/" + base + ".k.rapx";
    ASSERT_TRUE(std::filesystem::exists(sidecar)) << sidecar;
    EXPECT_EQ(static_cast<int64_t>(std::filesystem::file_size(sidecar)), w.writer->rap_export_stats().sidecar_bytes);
    parquet::RapIndex::Identity id{base, static_cast<uint64_t>(w.result.file_statistics.file_size), static_cast<uint64_t>(kRows), "k", 7};
    auto r = parquet::RapIndex::load(sidecar, id);
    ASSERT_EQ(r.state, parquet::RapIndex::State::READY) << r.reason;
    EXPECT_EQ(r.index->granularity_rows(), G);
    EXPECT_EQ(r.index->num_values(), 42u); // V10..V49 (40; the second half's ten values are among them) + V15RARE + V17TAIL
    std::set<std::string> values;
    for (int64_t row = 0; row < kRows; ++row) values.insert(written_value(row));
    EXPECT_EQ(values.size(), r.index->num_values());
    for (const auto& wv : values) {
        auto got = r.index->lookup({wv});
        auto want = expected_ranges(wv);
        ASSERT_EQ(got.size(), want.size()) << wv;
        for (size_t i = 0; i < got.size(); ++i) {
            EXPECT_EQ(got[i].start_row, want[i].start_row) << wv;
            EXPECT_EQ(got[i].end_row, want[i].end_row) << wv;
        }
    }
    // the last, partial bucket ends exactly at the row count; the rare row's bucket is the only one for RARE
    auto tail = r.index->lookup({"V17TAIL"});
    ASSERT_EQ(tail.size(), 1u);
    EXPECT_EQ(tail[0].start_row, 80000);
    EXPECT_EQ(tail[0].end_row, kRows);
    auto rare = r.index->lookup({"V15RARE"});
    ASSERT_EQ(rare.size(), 1u);
    EXPECT_EQ(rare[0].start_row, 60000);
    EXPECT_EQ(rare[0].end_row, 80000);
    // raw (lower-case) values were never written and are not indexed; absent values are empty
    EXPECT_TRUE(r.index->lookup({"v12"}).empty());
    EXPECT_TRUE(r.index->lookup({"v15rare"}).empty());
    EXPECT_TRUE(r.index->lookup({"V16NOPE"}).empty());
    // retain a copy for the harness decoder when asked
    if (const char* keep = std::getenv("RAP_EXPORT_RETAIN_DIR"); keep != nullptr && *keep) {
        std::filesystem::create_directories(keep);
        std::filesystem::copy_file(sidecar, std::string(keep) + "/" + base + ".k.rapx", std::filesystem::copy_options::overwrite_existing);
        std::filesystem::copy_file(w.path, std::string(keep) + "/" + base, std::filesystem::copy_options::overwrite_existing);
    }
}

// slice 3b. Encoding: adjacent buckets are one range in the sidecar's BYTES. The formula gives, per value, the
//    maximal runs of adjacent buckets (expected_ranges merges as lookup does); the encoded range count must equal
//    their sum, and be strictly below the bucket count (rows [0, 60000) cycle 40 values through every bucket, so
//    every one of those values spans three adjacent buckets = one run). lookup() cannot see this: it merges.
TEST_F(RapSidecarBuilderTest, AdjacentBucketsMerge) {
    Written w = write_file(_dir + "/rapx", {"k"});
    ASSERT_TRUE(w.result.io_status.ok()) << w.result.io_status.message();
    const std::string base = basename(w.path);
    const std::string sidecar = _dir + "/rapx/" + base + ".k.rapx";
    parquet::RapIndex::Identity id{base, static_cast<uint64_t>(w.result.file_statistics.file_size), static_cast<uint64_t>(kRows), "k", 7};
    auto r = parquet::RapIndex::load(sidecar, id);
    ASSERT_EQ(r.state, parquet::RapIndex::State::READY) << r.reason;
    std::set<std::string> values;
    for (int64_t row = 0; row < kRows; ++row) values.insert(written_value(row));
    size_t merged = 0, buckets = 0;
    for (const auto& wv : values) {
        merged += expected_ranges(wv).size();
        std::set<uint32_t> bs;
        for (int64_t row = 0; row < kRows; ++row) {
            if (written_value(row) == wv) bs.insert(static_cast<uint32_t>(row / G));
        }
        buckets += bs.size();
    }
    EXPECT_EQ(r.index->num_ranges(), merged) << "the encoded ranges must be the merged runs, not one per bucket";
    EXPECT_LT(merged, buckets) << "the fixture must have adjacent buckets to merge, or this case proves nothing";
    // the bytes follow: 16 bytes per range saved
    EXPECT_EQ(static_cast<int64_t>(std::filesystem::file_size(sidecar)), w.writer->rap_export_stats().sidecar_bytes);
}

// 2. Identity: the same bytes refuse every mismatch the reader checks.
TEST_F(RapSidecarBuilderTest, IdentityRefusals) {
    Written w = write_file(_dir + "/rapx", {"k"});
    ASSERT_TRUE(w.result.io_status.ok());
    const std::string base = basename(w.path);
    const std::string sidecar = _dir + "/rapx/" + base + ".k.rapx";
    const uint64_t size = w.result.file_statistics.file_size;
    auto expect_unusable = [&](parquet::RapIndex::Identity id, const char* why) {
        auto r = parquet::RapIndex::load(sidecar, id);
        EXPECT_EQ(r.state, parquet::RapIndex::State::UNUSABLE) << why;
        EXPECT_NE(r.reason.find(why), std::string::npos) << r.reason;
    };
    expect_unusable({base, size + 1, kRows, "k", 7}, "file_size");
    expect_unusable({base, size, kRows - 1, "k", 7}, "file_rows");
    expect_unusable({base, size, kRows, "v", 7}, "column");
    expect_unusable({base, size, kRows, "k", 9}, "field_id");
    expect_unusable({"other.parquet", size, kRows, "k", 7}, "file_name");
    EXPECT_EQ(parquet::RapIndex::load(sidecar, {base, size, kRows, "k", 7}).state, parquet::RapIndex::State::READY);
}

// 3. Consumer parity: the real reader consults the export-time sidecar; rows identical to the unindexed
//    read; the absent literal filters the file; the rare literal narrows planned IO.
TEST_F(RapSidecarBuilderTest, ConsumerParity) {
    Written w = write_file(_dir + "/rapx", {"k"});
    ASSERT_TRUE(w.result.io_status.ok());
    struct Case {
        const char* literal;
        size_t rows;
        bool narrows;
    // V12: rows < 60000 only (buckets 0-2) -> narrows. V10: 1500 rows in the first half + 4000 in the second, so it
    // sits in every bucket -> the index must NOT change the planned IO. V15RARE / V17TAIL: one bucket each.
    // V16NOPE: absent (inside the lexical range, so only the dictionary filter or the index can exclude it).
    } cases[] = {{"V12", 1500, true}, {"V10", 5500, false}, {"V15RARE", 1, true}, {"V17TAIL", 1, true}, {"V16NOPE", 0, true}};
    for (const auto& c : cases) {
        SCOPED_TRACE(c.literal);
        const Read base = read(w.path, c.literal, "");
        const Read idx = read(w.path, c.literal, _dir + "/rapx");
        LOG(INFO) << "RAP export parity literal=" << c.literal << " rows=" << base.rows.size() << " planned_bytes base="
                  << base.planned_bytes << " indexed=" << idx.planned_bytes << " consulted=" << idx.consulted
                  << " ready=" << idx.ready << " unusable=" << idx.unusable << " ranges=" << idx.ranges;
        EXPECT_EQ(base.rows.size(), c.rows);
        EXPECT_EQ(idx.rows, base.rows) << "indexed read differs from the unindexed read";
        EXPECT_EQ(base.consulted, 0);
        EXPECT_EQ(idx.consulted, 2);
        EXPECT_EQ(idx.unusable, 0);
        if (c.rows == 0) {
            EXPECT_TRUE(idx.file_filtered);
            EXPECT_EQ(idx.planned_bytes, 0);
        } else {
            EXPECT_EQ(idx.ready, 2);
            EXPECT_GT(idx.ranges, 0);
            if (c.narrows) {
                EXPECT_LT(idx.planned_bytes, base.planned_bytes) << "no narrowing";
            } else {
                EXPECT_EQ(idx.planned_bytes, base.planned_bytes) << "a value present in every bucket must not change the IO";
            }
        }
    }
}

// 4. Off by default: no dir -> nothing; dir but an unlisted / non-string column -> nothing.
TEST_F(RapSidecarBuilderTest, OffByDefault) {
    Written a = write_file("", {"k"});
    ASSERT_TRUE(a.result.io_status.ok());
    EXPECT_EQ(a.writer->rap_export_stats().sidecars_written, 0);
    EXPECT_EQ(a.writer->rap_export_stats().sidecar_bytes, 0);
    EXPECT_FALSE(std::filesystem::exists(_dir + "/rapx/" + basename(a.path) + ".k.rapx"));
    Written b = write_file(_dir + "/rapx", {"zzz", "v"});
    ASSERT_TRUE(b.result.io_status.ok());
    EXPECT_EQ(b.writer->rap_export_stats().sidecars_written, 0);
    EXPECT_TRUE(std::filesystem::is_empty(_dir + "/rapx"));
}

// 5. Rollback removes the data file AND its sidecar.
TEST_F(RapSidecarBuilderTest, RollbackRemovesSidecar) {
    Written w = write_file(_dir + "/rapx", {"k"});
    ASSERT_TRUE(w.result.io_status.ok());
    const std::string sidecar = _dir + "/rapx/" + basename(w.path) + ".k.rapx";
    ASSERT_TRUE(std::filesystem::exists(sidecar));
    ASSERT_TRUE(std::filesystem::exists(w.path));
    w.result.rollback_action();
    EXPECT_FALSE(std::filesystem::exists(w.path));
    EXPECT_FALSE(std::filesystem::exists(sidecar)) << "the sidecar must go with its data file";
}

// 6. Advisory: an unwritable sidecar directory does not fail the data file.
TEST_F(RapSidecarBuilderTest, AdvisoryFailure) {
    Written w = write_file("/proc/rap_unwritable_dir", {"k"});
    EXPECT_TRUE(w.result.io_status.ok()) << w.result.io_status.message();
    EXPECT_EQ(w.result.file_statistics.record_count, kRows);
    EXPECT_EQ(w.writer->rap_export_stats().failures, 1);
    EXPECT_EQ(w.writer->rap_export_stats().sidecars_written, 0);
    // the data file still reads, unindexed and complete
    const Read base = read(w.path, "V12", "");
    EXPECT_EQ(base.rows.size(), 1500u);
}

// 7. astra CX-36: the indexed column is evaluated exactly once per chunk -- by the row-group writer -- and the
//    sidecar is built from that very result. An evaluator that fails on a second call for the same chunk proves it:
//    the write succeeds, the call count equals the chunk count, and the sidecar holds the written (upper-cased) values.
TEST_F(RapSidecarBuilderTest, EvaluatorRunsOnce) {
    int calls = 0;
    std::set<const Chunk*> seen;
    auto eval = std::make_unique<SecondCallFailsEvaluator>(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), &calls, &seen);
    Written w = write_file(_dir + "/rapx", {"k"}, true, std::move(eval));
    ASSERT_TRUE(w.result.io_status.ok()) << w.result.io_status.message();
    EXPECT_EQ(calls, static_cast<int>(kRows / kChunk)) << "the indexed column must be evaluated exactly once per chunk";
    EXPECT_EQ(w.result.file_statistics.record_count, kRows);
    EXPECT_EQ(w.writer->rap_export_stats().sidecars_written, 1);
    const std::string base = basename(w.path);
    parquet::RapIndex::Identity id{base, static_cast<uint64_t>(w.result.file_statistics.file_size), static_cast<uint64_t>(kRows), "k", 7};
    auto r = parquet::RapIndex::load(_dir + "/rapx/" + base + ".k.rapx", id);
    ASSERT_EQ(r.state, parquet::RapIndex::State::READY) << r.reason;
    EXPECT_EQ(r.index->num_values(), 42u);
    for (const auto& wv : {std::string("V12"), std::string("V15RARE"), std::string("V17TAIL")}) {
        auto got = r.index->lookup({wv});
        auto want = expected_ranges(wv);
        ASSERT_EQ(got.size(), want.size()) << wv;
        for (size_t i = 0; i < got.size(); ++i) {
            EXPECT_EQ(got[i].start_row, want[i].start_row) << wv;
            EXPECT_EQ(got[i].end_row, want[i].end_row) << wv;
        }
    }
    // and the real reader agrees: indexed rows == unindexed rows for a literal the written file holds
    const Read base_r = read(w.path, "V12", "");
    const Read idx = read(w.path, "V12", _dir + "/rapx");
    EXPECT_EQ(base_r.rows.size(), 1500u);
    EXPECT_EQ(idx.rows, base_r.rows);
    EXPECT_EQ(idx.ready, 2);
}

} // namespace starrocks::formats
