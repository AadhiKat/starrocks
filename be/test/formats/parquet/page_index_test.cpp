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

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <random>
#include <vector>

#include "base/utility/defer_op.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "column/column_helper.h"
#include "common/config_exec_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "connector/hive/scanner/hdfs_scanner.h"
#include "exec/exec_env.h"
#include "exprs/binary_predicate.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/page_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/runtime_state.h"

namespace starrocks::parquet {

static FormatScannerStats g_hdfs_stats;
using starrocks::HdfsScannerContext;

class PageIndexTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
        _fragment_dict_state = std::make_unique<FragmentDictState>();
        _runtime_state->set_fragment_dict_state(_fragment_dict_state.get());
    }

    void TearDown() override {}

protected:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path);

    HdfsScannerContext* _create_scan_context();

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0);
    static void _set_scan_range(HdfsScannerContext* ctx, THdfsScanRange* scan_range);

    HdfsScannerContext* _create_file_random_read_context(const std::string& file_path);
    HdfsScannerContext* _create_file_only_c0_context(const std::string& file_path);
    HdfsScannerContext* _create_file_c0_c1_c2_context(const std::string& file_path);

    RuntimeState* _runtime_state = nullptr;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
    ObjectPool _pool;

    HdfsScannerContext _scanner_ctx;
};

std::unique_ptr<RandomAccessFile> PageIndexTest::_create_file(const std::string& file_path) {
    return *FileSystem::Default()->new_random_access_file(file_path);
}

HdfsScannerContext* PageIndexTest::_create_scan_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    ctx->format_scan_context.lazy_column_coalesce_counter = lazy_column_coalesce_counter;

    ctx->format_scan_context.timezone = "Asia/Shanghai";
    ctx->format_scan_context.stats = &g_hdfs_stats;
    ctx->format_scan_context.options.parquet_page_index_enable = true;
    ctx->format_scan_context.options.parquet_bloom_filter_enable = true;
    ctx->format_scan_context.predicate_tree = &ctx->predicates.predicate_tree;
    return ctx;
}

void PageIndexTest::_set_scan_range(HdfsScannerContext* ctx, THdfsScanRange* scan_range) {
    ctx->scan_range = scan_range;
    ctx->format_scan_context.scan_range_offset = scan_range->offset;
    ctx->format_scan_context.scan_range_length = scan_range->length;
}

HdfsScannerContext* PageIndexTest::_create_file_random_read_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c3", type_array},
            {""},
    };

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->format_scan_context.materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    _set_scan_range(ctx, _create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_only_c0_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->format_scan_context.materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    _set_scan_range(ctx, _create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_c0_c1_c2_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {""},
    };
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->format_scan_context.materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    _set_scan_range(ctx, _create_scan_range(file_path));

    return ctx;
}

THdfsScanRange* PageIndexTest::_create_scan_range(const std::string& file_path, size_t scan_length) {
    auto* scan_range = _pool.add(new THdfsScanRange());

    scan_range->relative_path = file_path;
    scan_range->file_length = std::filesystem::file_size(file_path);
    scan_range->offset = 4;
    scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;

    return scan_range;
}

TEST_F(PageIndexTest, TestRandomReadWith2PageSize) {
    std::random_device rd;
    std::mt19937 rng(rd());

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_array, true), chunk->num_columns());

    // c0 = np.arange(1, 20001)
    // c1 = np.arange(20000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    // c0 = np.arange(1, 100001)
    // c1 = np.arange(100000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string big_page_file = "./be/test/formats/parquet/test_data/page_index_big_page.parquet";
    // same data with above but without dictionary
    const std::string repeated_no_dict_file = "./be/test//formats/parquet/test_data/page_index_repeated_nodict.parquet";

    std::vector<std::string> files = {small_page_file, big_page_file, repeated_no_dict_file};

    // for small page 1000 values / page
    // for big page 10000 values / page
    for (size_t index = 0; index < 3; index++) {
        const std::string& file_path = files[index];
        std::cout << "file_path: " << file_path << std::endl;

        std::uniform_int_distribution<int> dist_small(1, 20000);
        std::uniform_int_distribution<int> dist_big(1, 100000);

        std::vector<int> oprands;
        size_t expected_row = 0;
        auto _print_predicate = [&](bool single) {
            std::stringstream ss;
            ss << "expected_row: " << expected_row << " predicate: c0 > " << oprands[0] << " and c0 < " << oprands[1];
            if (single) {
                return ss.str();
            }
            ss << " and c1 > " << oprands[2] << " and c1 < " << oprands[3];
            return ss.str();
        };

        std::vector<bool> single_or_not{true, false};

        for (bool single_flag : single_or_not) {
            // use 2 to save ci's time, change bigger to test more case
            for (int32_t i = 0; i < 2; i++) {
                oprands.clear();
                for (int32_t j = 0; j < 4; j++) {
                    int num = index == 0 ? dist_small(rng) : dist_big(rng);
                    oprands.emplace_back(num);
                }
                for (int k : std::vector<int>{0, 2}) {
                    if (oprands[k] > oprands[k + 1]) {
                        int temp = oprands[k];
                        oprands[k] = oprands[k + 1];
                        oprands[k + 1] = temp;
                    }
                }

                auto ctx = _create_file_random_read_context(file_path);
                auto file = _create_file(file_path);
                ctx->format_scan_context.conjunct_ctxs_by_slot[0].clear();
                ctx->format_scan_context.conjuncts.min_max_ctxs.clear();

                if (single_flag) {
                    Utils::SlotDesc min_max_slots[] = {
                            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                            {""},
                    };
                    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

                    std::vector<TExpr> t_conjuncts;
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, oprands[0], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, oprands[1], &t_conjuncts);

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->format_scan_context.conjuncts.min_max_ctxs);
                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->format_scan_context.conjunct_ctxs_by_slot[0]);

                    expected_row = std::max(oprands[1] - oprands[0] - 1, 0);
                } else {
                    ctx->format_scan_context.conjunct_ctxs_by_slot[1].clear();
                    Utils::SlotDesc min_max_slots[] = {
                            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
                            {""},
                    };
                    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

                    std::vector<TExpr> t_conjuncts;
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, oprands[0], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, oprands[1], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, oprands[2], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 1, oprands[3], &t_conjuncts);

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->format_scan_context.conjuncts.min_max_ctxs);

                    std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0], t_conjuncts[1]};
                    std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[2], t_conjuncts[3]};

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0,
                                                        &ctx->format_scan_context.conjunct_ctxs_by_slot[0]);
                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1,
                                                        &ctx->format_scan_context.conjunct_ctxs_by_slot[1]);

                    int low_bound = std::max(oprands[0], index == 0 ? 20001 - oprands[3] : 100001 - oprands[3]);
                    int up_bound = std::min(oprands[1], index == 0 ? 20001 - oprands[2] : 100001 - oprands[2]);
                    expected_row = std::max(up_bound - low_bound - 1, 0);
                }

                std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag) << std::endl;

                auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                                std::filesystem::file_size(file_path));

                Status status = file_reader->init(&ctx->format_scan_context);
                ASSERT_TRUE(status.ok());
                size_t total_row_nums = 0;
                while (!status.is_end_of_file()) {
                    chunk->reset();
                    status = file_reader->get_next(&chunk);
                    chunk->check_or_die();
                    total_row_nums += chunk->num_rows();
                    if (!status.ok() && !status.is_end_of_file()) {
                        std::cout << status.message() << std::endl;
                        DCHECK(false) << "file path: " << file_path << ", " << _print_predicate(single_flag);
                    }
                    // check row value
                    if (chunk->num_rows() > 0) {
                        ColumnPtr c0 = chunk->get_column_by_index(0);
                        ColumnPtr c1 = chunk->get_column_by_index(1);
                        ColumnPtr c2 = chunk->get_column_by_index(2);
                        ColumnPtr c3 = chunk->get_column_by_index(3);
                        for (size_t row_index = 0; row_index < chunk->num_rows(); row_index++) {
                            int32_t c0_value = c0->get(row_index).get_int32();
                            int32_t c1_value = c1->get(row_index).get_int32();
                            bool flag = index == 0 ? c0_value + c1_value == 20001 : c0_value + c1_value == 100001;
                            if (c0_value % 10 == 0) {
                                flag &= c2->is_null(row_index);
                                flag &= c3->is_null(row_index);
                            } else {
                                flag &= (!c2->is_null(row_index));
                                flag &= (!c3->is_null(row_index));
                                if (!flag) {
                                    std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag);
                                }
                                EXPECT_TRUE(flag);
                                std::string expected_string = std::to_string(c0_value % 100);
                                Slice expected_value = Slice(expected_string);
                                Slice c2_value = c2->get(row_index).get_slice();
                                flag &= (c2_value == expected_value);
                                DatumArray c3_value = c3->get(row_index).get_array();
                                flag &= (c3_value.size() == 3) && (!c3_value[0].is_null()) &&
                                        (c3_value[0].get_int32() == (c0_value % 1000)) && (c3_value[1].is_null()) &&
                                        (!c3_value[2].is_null()) && (c3_value[2].get_int32() == (c1_value % 1000));
                            }
                            if (!flag) {
                                std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag);
                            }
                            EXPECT_TRUE(flag);
                        }
                    }
                }
                EXPECT_EQ(total_row_nums, expected_row);
            }
        }
    }
}

// RAP / lake-index row-range transport.
//
// astra CX-12 r1: ranges.size() is not a pruning metric -- an unpruned column emits ONE
//   whole-chunk range while a pruned one emits a range per selected page plus a
//   dictionary range, so pruning can INCREASE the count. Compare bytes and contents.
// astra CX-12 r2: planned ranges must be captured before consumption -- get_next() nulls
//   out row-group readers on exhaustion.
// astra CX-12 r3: init() prepares ONLY the first group; later groups are prepared in
//   get_next(). Collecting straight after init() mixes page-selected ranges (group 0)
//   with whole-chunk ranges (the rest). Prepare each group, and key bytes by the group's
//   PHYSICAL first row so a filtered earlier group cannot shift positional comparisons.
TEST_F(PageIndexTest, TestSelectedRowRangesSkipPages) {
    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    struct Result {
        int64_t planned_bytes = 0; // planned IO, not observed storage reads
        std::map<uint64_t, int64_t> bytes_by_first_row; // keyed by physical group identity
        size_t groups_kept = 0;
        size_t rows_returned = 0;
        std::vector<int32_t> values;
    };

    // c0 in (5500, 7500). Applied to whichever context needs it.
    auto apply_predicate = [&](HdfsScannerContext* c) {
        Utils::SlotDesc min_max_slots[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0}, {""}};
        c->format_scan_context.conjunct_ctxs_by_slot[0].clear();
        c->format_scan_context.conjuncts.min_max_ctxs.clear();
        c->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);
        std::vector<TExpr> t_conjuncts;
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 5500, &t_conjuncts);
        ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, 7500, &t_conjuncts);
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                            &c->format_scan_context.conjuncts.min_max_ctxs);
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                            &c->format_scan_context.conjunct_ctxs_by_slot[0]);
        Utils::SlotDesc slot_descs[] = {{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
        TupleDescriptor* td = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        std::vector<ExprContext*> all;
        for (auto* e : c->format_scan_context.conjuncts.min_max_ctxs) all.push_back(e);
        for (auto* e : c->format_scan_context.conjunct_ctxs_by_slot[0]) all.push_back(e);
        ParquetUTBase::setup_conjuncts_manager(all, nullptr, td, _runtime_state, c);
    };

    auto run = [&](std::vector<RowRangeHint> hint, bool with_predicate) {
        Result out;

        // Pass 1: accounting. This reader is never consumed, so released pointers are
        // unreachable and consumption cannot perturb the measurement.
        {
            auto ctx = _create_file_only_c0_context(small_page_file);
            auto file = _create_file(small_page_file);
            ctx->format_scan_context.selected_row_ranges = hint;
            if (with_predicate) apply_predicate(ctx);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                       std::filesystem::file_size(small_page_file));
            Status st = reader->init(&ctx->format_scan_context);
            EXPECT_TRUE(st.ok()) << st.message();
            out.groups_kept = reader->_row_group_readers.size();
            for (size_t gi = 0; gi < reader->_row_group_readers.size(); ++gi) {
                auto& rg = reader->_row_group_readers[gi];
                if (rg == nullptr) continue;
                if (gi > 0) { // init() already prepared group 0
                    Status ps = rg->prepare();
                    EXPECT_TRUE(ps.ok()) << ps.message();
                }
                std::vector<SharedBufferedInputStream::IORange> ranges;
                int64_t end_offset = 0;
                rg->collect_io_ranges(&ranges, &end_offset);
                int64_t g = 0;
                for (const auto& r : ranges) g += r.size;
                out.bytes_by_first_row[rg->get_row_group_first_row()] = g;
                out.planned_bytes += g;
            }
        }

        // Pass 2: contents, from a separate reader.
        {
            auto ctx = _create_file_only_c0_context(small_page_file);
            auto file = _create_file(small_page_file);
            ctx->format_scan_context.selected_row_ranges = hint;
            if (with_predicate) apply_predicate(ctx);
            auto reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                       std::filesystem::file_size(small_page_file));
            Status st = reader->init(&ctx->format_scan_context);
            EXPECT_TRUE(st.ok()) << st.message();
            while (true) { // an empty chunk must not terminate a multi-group read
                auto chunk = std::make_shared<Chunk>();
                chunk->append_column(
                        ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                        chunk->num_columns());
                Status s2 = reader->get_next(&chunk);
                if (s2.is_end_of_file()) break;
                EXPECT_TRUE(s2.ok()) << s2.message();
                for (size_t r = 0; r < chunk->num_rows(); r++) {
                    out.values.push_back(chunk->get_column_by_index(0)->get(r).get_int32());
                }
                out.rows_returned += chunk->num_rows();
            }
        }
        return out;
    };

    const Result base = run({}, false);
    ASSERT_GT(base.rows_returned, 0u);
    ASSERT_GT(base.planned_bytes, 0);
    ASSERT_GE(base.bytes_by_first_row.size(), 2u) << "fixture must exercise more than one row group";

    // A narrow band plans fewer bytes and returns exactly those rows.
    const Result hinted = run({RowRangeHint{5000, 8000}}, false);
    EXPECT_LT(hinted.planned_bytes, base.planned_bytes);
    EXPECT_EQ(hinted.rows_returned, 3000u);
    ASSERT_EQ(hinted.values.size(), 3000u);
    EXPECT_EQ(hinted.values.front(), 5001); // c0 = arange(1, 20001)
    EXPECT_EQ(hinted.values.back(), 8000);
    EXPECT_EQ(std::vector<int32_t>(base.values.begin() + 5000, base.values.begin() + 8000), hinted.values);

    // Disjoint bands.
    const Result split = run({RowRangeHint{1000, 2000}, RowRangeHint{15000, 16000}}, false);
    EXPECT_EQ(split.rows_returned, 2000u);
    EXPECT_LT(split.planned_bytes, base.planned_bytes);
    ASSERT_EQ(split.values.size(), 2000u);
    EXPECT_EQ(split.values.front(), 1001);
    EXPECT_EQ(split.values[1000], 15001);
    // per PHYSICAL group, and STRICTLY less wherever the group survives in both:
    // equality would be satisfied by a regression that restores whole-chunk reads.
    for (const auto& [first_row, bytes] : split.bytes_by_first_row) {
        auto it = base.bytes_by_first_row.find(first_row);
        ASSERT_NE(it, base.bytes_by_first_row.end());
        EXPECT_LT(bytes, it->second) << "row group at first_row=" << first_row
                                     << " fetched whole chunks despite a partial selection";
    }

    // A LATER group, partially selected, with earlier groups filtered. This is the case
    // that catches a regression confined to later-group page selection: positional
    // comparison would be wrong here, which is why keys are physical first-row values.
    const Result late_partial = run({RowRangeHint{12000, 13000}}, false);
    EXPECT_EQ(late_partial.rows_returned, 1000u);
    ASSERT_EQ(late_partial.bytes_by_first_row.size(), 1u);
    {
        const auto& [first_row, bytes] = *late_partial.bytes_by_first_row.begin();
        auto it = base.bytes_by_first_row.find(first_row);
        ASSERT_NE(it, base.bytes_by_first_row.end()) << "surviving group must exist in the baseline";
        EXPECT_LT(bytes, it->second) << "later-group page selection did not reduce planned bytes";
    }

    // astra CX-11: a hint missing every group must FILTER them, not leave an empty range
    // for select_offset_index() to index into.
    const Result none = run({RowRangeHint{9000000, 9000100}}, false);
    EXPECT_EQ(none.rows_returned, 0u);
    EXPECT_EQ(none.groups_kept, 0u);

    // No hint is byte-for-byte and content-for-content the previous behaviour.
    const Result again = run({}, false);
    EXPECT_EQ(again.planned_bytes, base.planned_bytes);
    EXPECT_EQ(again.values, base.values);

    // Malformed intervals are ignored; never row loss.
    const Result bad = run({RowRangeHint{500, 500}, RowRangeHint{900, 100}}, false);
    EXPECT_EQ(bad.planned_bytes, base.planned_bytes);
    EXPECT_EQ(bad.values, base.values);

    // astra CX-11 composition, OVERLAPPING: predicate c0 in (5500,7500), hint [6000,9000).
    const Result pred_only = run({}, true);
    ASSERT_GT(pred_only.rows_returned, 0u);
    const Result composed = run({RowRangeHint{6000, 9000}}, true);
    ASSERT_GT(composed.rows_returned, 0u);
    for (int32_t v : composed.values) {
        EXPECT_GT(v, 5500);
        EXPECT_LT(v, 7500);
        EXPECT_GT(v, 6000) << "row below the hint start survived: composition replaced instead of intersected";
    }
    EXPECT_LT(composed.rows_returned, pred_only.rows_returned);

    // astra CX-11 composition, DISJOINT: predicate (5500,7500) vs hint [8000,9000).
    const Result disjoint = run({RowRangeHint{8000, 9000}}, true);
    EXPECT_EQ(disjoint.rows_returned, 0u)
            << "disjoint predicate and hint returned rows: composition replaced instead of intersected";
}

// astra CX-12: the scanner-boundary conversion needs its own control, because the test
// above injects FormatScanContext directly. NOTE: this covers the conversion FUNCTION,
// not its call site in HdfsScanner::_build_scanner_context(), which remains untested.
TEST_F(PageIndexTest, TestBuildRowRangeHintsConversion) {
    auto mk = [](int64_t s, int64_t e) {
        TRowRange r;
        r.start_row = s;
        r.end_row = e;
        return r;
    };
    std::vector<RowRangeHint> out;

    build_row_range_hints({mk(10, 20), mk(30, 40)}, &out);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].start_row, 10);
    EXPECT_EQ(out[0].end_row, 20);
    EXPECT_EQ(out[1].start_row, 30);

    build_row_range_hints({mk(5, 5), mk(9, 2), mk(-7, -1)}, &out); // empty/inverted/negative
    EXPECT_TRUE(out.empty());

    build_row_range_hints({mk(-5, 3)}, &out); // negative start clamped, not rejected
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].start_row, 0);
    EXPECT_EQ(out[0].end_row, 3);

    build_row_range_hints({}, &out); // output always reset
    EXPECT_TRUE(out.empty());
}

// ---------------------------------------------------------------------------------------------
// page_index_small_page.parquet, row group 0, column c0, predicate 5500 < c0 < 7500.
// Everything asserted below was read out of the file itself, not assumed:
//
//   chunk        dictionary_page_offset 4, data_page_offset 40069, total_compressed_size 56040
//                => dictionary range [4, 40069) = 40065 bytes, chunk = [4, 56044)
//   offset index 10 pages of 1000 rows each:
//                  0: 40069/1285   1: 41354/1410   2: 42764/1535   3: 44299/1535   4: 45834/1660
//                  5: 47494/1660   6: 49154/1660   7: 50814/1660   8: 52474/1785   9: 54259/1785
//   selected     pages 5, 6, 7 (rows 5000..7999), contiguous, 3 x 1660 = 4980 bytes
//   coverage     (40065 + 4980) / 56040 = 0.8038
// ---------------------------------------------------------------------------------------------
namespace {
constexpr int64_t kC0ChunkStart = 4;
constexpr int64_t kC0DataPageOffset = 40069;
constexpr int64_t kC0ChunkCompressedSize = 56040;
constexpr int64_t kC0ChunkEnd = kC0ChunkStart + kC0ChunkCompressedSize; // 56044
constexpr int64_t kC0DictSize = kC0DataPageOffset - kC0ChunkStart;      // 40065
constexpr int64_t kC0SelectedPageBytes = 3 * 1660;                      // 4980
constexpr double kC0Coverage = static_cast<double>(kC0DictSize + kC0SelectedPageBytes) /
                               static_cast<double>(kC0ChunkCompressedSize); // 0.8038
// Pages 5..7 are contiguous, so they merge into one run [47494, 52474). Padding then covers the
// page-header peek taken at page 7's start: min(50814 + 16384, 56044) = 56044, i.e. the chunk end.
constexpr int64_t kC0MergedRunOffset = 47494;
constexpr int64_t kC0MergedRunSize = kC0ChunkEnd - kC0MergedRunOffset; // 8550
} // namespace

TEST_F(PageIndexTest, TestCollectIORangeWithPageIndex) {
    // What this test asserted BEFORE range-run merging and header padding existed. Kept verbatim so
    // that the change is visible in the diff rather than silently rewritten, and used below as a
    // negative case: the three per-page ranges and this end_offset must no longer be produced. The
    // dictionary range is listed for completeness and is deliberately still expected.
    const std::vector<std::pair<int64_t, int64_t>> kHistoricalUnmergedRanges = {
            {kC0ChunkStart, kC0DictSize}, // dict page, emitted on its own
            {47494, 1660},                // page 5, no padding
            {49154, 1660},                // page 6
            {50814, 1660},                // page 7, range stops dead at the last selected byte
    };
    const int64_t kHistoricalEndOffset = 52474;

    struct Observed {
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        int64_t io_range_count = 0;
        int64_t io_range_merged = 0;
        int64_t whole_chunk_fallback = 0;
        size_t rows = 0;
        std::vector<int32_t> values;
    };

    // One full read of the file at a given coverage threshold, returning both the registered ranges
    // and the rows actually decoded. The rows are the point: whatever the reader decides to FETCH,
    // it must decode exactly the rows the predicate selects.
    auto run = [&](double min_coverage) {
        const double saved_coverage = config::parquet_page_select_min_coverage;
        config::parquet_page_select_min_coverage = min_coverage;
        DeferOp restore([&]() { config::parquet_page_select_min_coverage = saved_coverage; });

        Observed obs;
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                chunk->num_columns());

        const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

        Utils::SlotDesc min_max_slots[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                {""},
        };

        auto ctx = _create_file_only_c0_context(small_page_file);
        auto file = _create_file(small_page_file);
        ctx->format_scan_context.conjunct_ctxs_by_slot[0].clear();
        ctx->format_scan_context.conjuncts.min_max_ctxs.clear();
        ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

        std::vector<TExpr> t_conjuncts;
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 5500, &t_conjuncts);
        ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, 7500, &t_conjuncts);

        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                            &ctx->format_scan_context.conjuncts.min_max_ctxs);
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                            &ctx->format_scan_context.conjunct_ctxs_by_slot[0]);

        // tuple desc
        Utils::SlotDesc slot_descs[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                {""},
        };
        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        std::vector<ExprContext*> all_conjuncts{};
        for (auto* expr : ctx->format_scan_context.conjuncts.min_max_ctxs) {
            all_conjuncts.push_back(expr);
        }
        for (auto* expr : ctx->format_scan_context.conjunct_ctxs_by_slot[0]) {
            all_conjuncts.push_back(expr);
        }
        ParquetUTBase::setup_conjuncts_manager(all_conjuncts, nullptr, tuple_desc, _runtime_state, ctx);

        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(small_page_file));

        Status status = file_reader->init(&ctx->format_scan_context);
        EXPECT_TRUE(status.ok()) << status.message();

        // two row groups, but one is filtered.
        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        // The range-registration counters are cumulative on a shared FormatScannerStats, and init()
        // has already run prepare() on group 0 -- which registers ranges through this same code. So
        // the baseline goes HERE, around the explicit call, or every number would be doubled.
        const int64_t base_count = ctx->format_scan_context.stats->page_io_range_count;
        const int64_t base_merged = ctx->format_scan_context.stats->page_io_range_merged;
        const int64_t base_fallback = ctx->format_scan_context.stats->page_whole_chunk_fallback;

        file_reader->_row_group_readers[0]->collect_io_ranges(&obs.ranges, &obs.end_offset);
        // 3 pages, 1 range 5000-8000
        EXPECT_EQ(file_reader->_row_group_readers[0]->_range.size(), 1);

        obs.io_range_count = ctx->format_scan_context.stats->page_io_range_count - base_count;
        obs.io_range_merged = ctx->format_scan_context.stats->page_io_range_merged - base_merged;
        obs.whole_chunk_fallback = ctx->format_scan_context.stats->page_whole_chunk_fallback - base_fallback;

        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            chunk->check_or_die();
            obs.rows += chunk->num_rows();
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                obs.values.push_back(chunk->get_column_by_index(0)->get(i).get_int32());
            }
        }
        return obs;
    };

    // The rows the predicate selects, independent of any I/O decision: c0 is 1..10000 in row group 0,
    // so 5500 < c0 < 7500 is exactly 5501..7499.
    std::vector<int32_t> expected_values;
    for (int32_t v = 5501; v <= 7499; v++) {
        expected_values.push_back(v);
    }

    {
        SCOPED_TRACE("MergedAndPaddedRanges: coverage fallback disabled, so this is the merge path only");
        auto obs = run(2.0); // > 1.0 disables the whole-chunk fallback entirely

        // The dictionary page keeps its own range, exactly as before. The three contiguous selected
        // pages become ONE range, whose end is extended so that the page-header peek taken at the
        // start of page 7 is contained -- here that reaches the chunk end, because only 5,230 bytes
        // of chunk remain past page 7's start and the peek wants 16 KB.
        ASSERT_EQ(obs.ranges.size(), 2);
        EXPECT_EQ(obs.ranges[0].offset, kC0ChunkStart);
        EXPECT_EQ(obs.ranges[0].size, kC0DictSize);
        EXPECT_EQ(obs.ranges[1].offset, kC0MergedRunOffset);
        EXPECT_EQ(obs.ranges[1].size, kC0MergedRunSize);
        EXPECT_EQ(obs.end_offset, kC0ChunkEnd);
        // Still strictly less than reading the chunk whole -- the point of the page path.
        EXPECT_LT(obs.ranges[0].size + obs.ranges[1].size, kC0ChunkCompressedSize);

        // Produced by merging, NOT by the coverage fallback. Without this assertion the two mechanisms
        // are hard to tell apart from the outside, and a reverted merge would still look plausible.
        EXPECT_EQ(obs.io_range_count, 1);  // one run emitted for the three selected pages
        EXPECT_EQ(obs.io_range_merged, 2); // pages 6 and 7 absorbed into page 5's run
        EXPECT_EQ(obs.whole_chunk_fallback, 0);

        // Negative case: the pre-change emission is gone, and gone for both reasons. The dictionary
        // range is excluded on purpose -- it is unchanged and must stay unchanged.
        EXPECT_NE(obs.end_offset, kHistoricalEndOffset) << "the page-header peek was not padded for";
        for (size_t i = 1; i < kHistoricalUnmergedRanges.size(); i++) {
            const auto& [offset, size] = kHistoricalUnmergedRanges[i];
            for (const auto& r : obs.ranges) {
                EXPECT_FALSE(r.offset == offset && r.size == size)
                        << "historical unmerged page range (" << offset << ", " << size << ") still emitted";
            }
        }

        // Fetching differently must not decode differently.
        EXPECT_EQ(obs.rows, 1999u);
        EXPECT_EQ(obs.values, expected_values);
    }

    {
        SCOPED_TRACE("WholeChunkFallbackAtTheDefaultThreshold");
        auto obs = run(0.8); // the shipped default; this chunk's coverage is 0.8038

        ASSERT_EQ(obs.ranges.size(), 1);
        EXPECT_EQ(obs.ranges[0].offset, kC0ChunkStart);
        EXPECT_EQ(obs.ranges[0].size, kC0ChunkCompressedSize);
        EXPECT_EQ(obs.end_offset, kC0ChunkEnd);

        // Same range, different mechanism -- and the counters say which.
        EXPECT_EQ(obs.whole_chunk_fallback, 1);
        EXPECT_EQ(obs.io_range_count, 0);
        EXPECT_EQ(obs.io_range_merged, 0);

        EXPECT_EQ(obs.rows, 1999u);
        EXPECT_EQ(obs.values, expected_values);
    }

    {
        SCOPED_TRACE("CoverageThresholdBoundary");
        // 0.8038 is the real coverage of this chunk. Just below it the fallback fires, just above it
        // the merge path runs, and the decoded rows are identical either way.
        EXPECT_NEAR(kC0Coverage, 0.8038, 1e-4);

        auto below = run(0.8035);
        EXPECT_EQ(below.whole_chunk_fallback, 1);
        EXPECT_EQ(below.io_range_count, 0);
        ASSERT_EQ(below.ranges.size(), 1);
        EXPECT_EQ(below.ranges[0].size, kC0ChunkCompressedSize);
        EXPECT_EQ(below.values, expected_values);

        auto above = run(0.8045);
        EXPECT_EQ(above.whole_chunk_fallback, 0);
        EXPECT_EQ(above.io_range_count, 1);
        ASSERT_EQ(above.ranges.size(), 2);
        EXPECT_EQ(above.ranges[1].offset, kC0MergedRunOffset);
        EXPECT_EQ(above.values, expected_values);
    }
}

// The index-OFF shape: no predicate, so _range spans the whole row group, select_offset_index() is
// never called, _offset_index_ctx stays null and collect_column_io_range() takes the else branch.
// That branch is byte-for-byte untouched by this work and this case is what says so.
TEST_F(PageIndexTest, TestCollectIORangeWithoutPageSelectionIsUnchanged) {
    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    // No conjuncts at all: exactly the shape TestSelectedRowRangesSkipPages uses for its baseline.
    auto ctx = _create_file_only_c0_context(small_page_file);
    auto file = _create_file(small_page_file);

    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(small_page_file));

    Status status = file_reader->init(&ctx->format_scan_context);
    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_EQ(file_reader->_row_group_readers.size(), 2);

    // Baseline after init(), which has already prepared group 0 through this same code.
    const int64_t base_count = ctx->format_scan_context.stats->page_io_range_count;
    const int64_t base_merged = ctx->format_scan_context.stats->page_io_range_merged;
    const int64_t base_fallback = ctx->format_scan_context.stats->page_whole_chunk_fallback;

    std::vector<SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);

    // Exactly one range: [dictionary_page_offset, +total_compressed_size).
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, kC0ChunkStart);
    EXPECT_EQ(ranges[0].size, kC0ChunkCompressedSize);
    EXPECT_EQ(end_offset, kC0ChunkEnd);

    // None of the page-path machinery ran, so none of its counters moved.
    EXPECT_EQ(ctx->format_scan_context.stats->page_io_range_count - base_count, 0);
    EXPECT_EQ(ctx->format_scan_context.stats->page_io_range_merged - base_merged, 0);
    EXPECT_EQ(ctx->format_scan_context.stats->page_whole_chunk_fallback - base_fallback, 0);
}

// Direct tests of the range emission itself, on a synthetic offset index, so that the geometry under
// test is stated in the test rather than depending on what some fixture file happens to contain.
namespace {

ColumnOffsetIndexCtx make_offset_index_ctx(const std::vector<std::pair<int64_t, int32_t>>& pages,
                                           const std::vector<bool>& selected) {
    ColumnOffsetIndexCtx ctx;
    ctx.rg_first_row = 0;
    int64_t first_row = 0;
    for (const auto& [offset, size] : pages) {
        tparquet::PageLocation loc;
        loc.__set_offset(offset);
        loc.__set_compressed_page_size(size);
        loc.__set_first_row_index(first_row);
        ctx.offset_index.page_locations.emplace_back(loc);
        first_row += 1000;
    }
    ctx.page_selected = selected;
    return ctx;
}

} // namespace

TEST_F(PageIndexTest, TestCollectIORangeMergesRunsWithinDistance) {
    constexpr int64_t kMB = 1024 * 1024;
    // Two 4 KB pages with 1.5 MB of unselected data between them.
    const int64_t first_offset = 1000000;
    const int64_t second_offset = first_offset + 4096 + 1500000;
    auto ctx = make_offset_index_ctx({{first_offset, 4096}, {second_offset, 4096}}, {true, true});

    auto emit = [&](int64_t merge_max_distance) {
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        PageIORangeOptions opts;
        opts.chunk_end = 10 * 1000 * 1000; // far away, so nothing is clamped by it
        opts.merge_max_distance = merge_max_distance;
        opts.header_peek_size = kDefaultPageHeaderSize;
        ctx.collect_io_range(&ranges, &end_offset, true, opts);
        return ranges;
    };

    {
        // 1 MB apart is the deployed io_coalesce_read_max_distance_size. A 1.5 MB gap breaks the run,
        // which is the behaviour that costs a second remote round trip.
        auto ranges = emit(1 * kMB);
        ASSERT_EQ(ranges.size(), 2);
        // Each range runs from its page start to that page start plus the peek (the page itself is
        // only 4 KB, so the peek is what sets the end).
        EXPECT_EQ(ranges[0].offset, first_offset);
        EXPECT_EQ(ranges[0].size, kDefaultPageHeaderSize);
        EXPECT_EQ(ranges[1].offset, second_offset);
        EXPECT_EQ(ranges[1].size, kDefaultPageHeaderSize);
    }
    {
        // Raise the bound past the gap and the same two pages become one request.
        auto ranges = emit(2 * kMB);
        ASSERT_EQ(ranges.size(), 1);
        EXPECT_EQ(ranges[0].offset, first_offset);
        EXPECT_EQ(ranges[0].size, (second_offset + kDefaultPageHeaderSize) - first_offset);
    }
}

TEST_F(PageIndexTest, TestCollectIORangePaddingNeverLeavesTheChunk) {
    constexpr int64_t kMB = 1024 * 1024;

    {
        // The chunk ends 60 bytes after the page starts, far short of the 16 KB peek. Padding stops
        // there: a range that ran past total_compressed_size would overlap the NEXT column's chunk,
        // and SharedBufferedInputStream::_sort_and_check_overlap() fails the whole row group on an
        // overlap.
        auto ctx = make_offset_index_ctx({{100, 50}}, {true});
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        PageIORangeOptions opts;
        opts.chunk_end = 160;
        opts.merge_max_distance = kMB;
        opts.header_peek_size = kDefaultPageHeaderSize;
        ctx.collect_io_range(&ranges, &end_offset, true, opts);
        ASSERT_EQ(ranges.size(), 1);
        EXPECT_EQ(ranges[0].offset, 100);
        EXPECT_EQ(ranges[0].size, 60);
        EXPECT_EQ(ranges[0].offset + ranges[0].size, opts.chunk_end);
        EXPECT_EQ(end_offset, opts.chunk_end);
    }
    {
        // With room to spare the full peek is added.
        auto ctx = make_offset_index_ctx({{100, 50}}, {true});
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        PageIORangeOptions opts;
        opts.chunk_end = 1000000;
        opts.merge_max_distance = kMB;
        opts.header_peek_size = kDefaultPageHeaderSize;
        ctx.collect_io_range(&ranges, &end_offset, true, opts);
        ASSERT_EQ(ranges.size(), 1);
        EXPECT_EQ(ranges[0].size, kDefaultPageHeaderSize);
    }
    {
        // And nothing at all is added when the page is already longer than the peek, which is the
        // common case for a StarRocks-written page. This is what keeps the padding from becoming a
        // read-amplification tax on normal files.
        auto ctx = make_offset_index_ctx({{0, 20000}}, {true});
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        PageIORangeOptions opts;
        opts.chunk_end = 1000000;
        opts.merge_max_distance = kMB;
        opts.header_peek_size = kDefaultPageHeaderSize;
        ctx.collect_io_range(&ranges, &end_offset, true, opts);
        ASSERT_EQ(ranges.size(), 1);
        EXPECT_EQ(ranges[0].offset, 0);
        EXPECT_EQ(ranges[0].size, 20000);
        EXPECT_EQ(end_offset, 20000);
    }
    {
        // Padding must also never reach the next emitted range. With a merge distance below the peek
        // size the two pages stay separate AND the first range stops exactly where the second begins.
        auto ctx = make_offset_index_ctx({{0, 100}, {1000, 100}}, {true, true});
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        PageIORangeOptions opts;
        opts.chunk_end = 1000000;
        opts.merge_max_distance = 200; // < the 900-byte gap, and far below the 16 KB peek
        opts.header_peek_size = kDefaultPageHeaderSize;
        ctx.collect_io_range(&ranges, &end_offset, true, opts);
        ASSERT_EQ(ranges.size(), 2);
        EXPECT_EQ(ranges[0].offset, 0);
        EXPECT_EQ(ranges[0].size, 1000);
        EXPECT_EQ(ranges[1].offset, 1000);
        EXPECT_EQ(ranges[1].size, kDefaultPageHeaderSize);
        // No overlap, which is what _sort_and_check_overlap() would otherwise reject.
        EXPECT_LE(ranges[0].offset + ranges[0].size, ranges[1].offset);
    }
}

TEST_F(PageIndexTest, TestCollectIORangeDefaultOptionsMatchTheHistoricalEmission) {
    // The default options object must reproduce the pre-change emission exactly: one range per
    // selected page, no padding, end_offset at the last selected byte. Anything that changes this is
    // a behaviour change for every caller that does not opt in.
    auto ctx = make_offset_index_ctx({{40069, 1285},
                                      {41354, 1410},
                                      {42764, 1535},
                                      {44299, 1535},
                                      {45834, 1660},
                                      {47494, 1660},
                                      {49154, 1660},
                                      {50814, 1660},
                                      {52474, 1785},
                                      {54259, 1785}},
                                     {false, false, false, false, false, true, true, true, false, false});

    std::vector<SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    ctx.collect_io_range(&ranges, &end_offset, true);

    ASSERT_EQ(ranges.size(), 3);
    EXPECT_EQ(ranges[0].offset, 47494);
    EXPECT_EQ(ranges[0].size, 1660);
    EXPECT_EQ(ranges[1].offset, 49154);
    EXPECT_EQ(ranges[1].size, 1660);
    EXPECT_EQ(ranges[2].offset, 50814);
    EXPECT_EQ(ranges[2].size, 1660);
    EXPECT_EQ(end_offset, 52474);
    for (const auto& r : ranges) {
        EXPECT_TRUE(r.is_active);
    }

    // selected_page_bytes() is what the coverage guard divides by total_compressed_size.
    EXPECT_EQ(ctx.selected_page_bytes(), 3 * 1660);
}

TEST_F(PageIndexTest, TestTwoColumnIntersectPageIndex) {
    auto test = [&]() {
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                chunk->num_columns());
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                chunk->num_columns());
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
                chunk->num_columns());

        const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

        Utils::SlotDesc min_max_slots[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
                {""},
        };

        auto ctx = _create_file_c0_c1_c2_context(small_page_file);
        auto file = _create_file(small_page_file);
        ctx->format_scan_context.conjunct_ctxs_by_slot[0].clear();
        ctx->format_scan_context.conjunct_ctxs_by_slot[1].clear();
        ctx->format_scan_context.conjuncts.min_max_ctxs.clear();
        ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

        std::vector<TExpr> t_conjuncts;
        // c0: 1->20000, c0 > 5000
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 5000, &t_conjuncts);
        // c1: 20000->1, c1 > 5000
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, 5000, &t_conjuncts);

        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                            &ctx->format_scan_context.conjuncts.min_max_ctxs);

        std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0]};
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0,
                                            &ctx->format_scan_context.conjunct_ctxs_by_slot[0]);

        std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[1]};
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1,
                                            &ctx->format_scan_context.conjunct_ctxs_by_slot[1]);

        // tuple desc
        Utils::SlotDesc slot_descs[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                {""},
        };
        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        std::vector<ExprContext*> all_conjuncts{};
        for (auto* expr : ctx->format_scan_context.conjuncts.min_max_ctxs) {
            all_conjuncts.push_back(expr);
        }
        for (auto* expr : ctx->format_scan_context.conjunct_ctxs_by_slot[0]) {
            all_conjuncts.push_back(expr);
        }
        for (auto* expr : ctx->format_scan_context.conjunct_ctxs_by_slot[1]) {
            all_conjuncts.push_back(expr);
        }
        ParquetUTBase::setup_conjuncts_manager(all_conjuncts, nullptr, tuple_desc, _runtime_state, ctx);

        auto shared_buffer = std::make_shared<SharedBufferedInputStream>(file->stream(), small_page_file,
                                                                         std::filesystem::file_size(small_page_file));
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(small_page_file), DataCacheOptions(),
                                                        shared_buffer.get());

        Status status = file_reader->init(&ctx->format_scan_context);
        ASSERT_TRUE(status.ok());

        // two row groups.
        EXPECT_EQ(file_reader->_row_group_readers.size(), 2);
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;

        for (auto& r : file_reader->_row_group_readers) {
            r->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
        }

        // collect io of column index and offset index for active column,
        // and offset index for lazy column
        // and two group collect together. (2 + 2 + 1) * 2 = 10
        EXPECT_EQ(ranges.size(), 10);

        ranges.clear();
        end_offset = 0;

        file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);
        // Pages 5..9 (rows 5001-10000) are selected in all three columns. This used to be
        // (5 pages + 1 dict page) x 3 columns = 18 ranges. It is now 4, and the reason differs per
        // column, which is the whole point of the two mechanisms being separately observable:
        //
        //   c0  dict 40,065 of 56,040 compressed bytes, 5 selected pages = 8,550
        //       -> coverage 0.8675 >= parquet_page_select_min_coverage, whole chunk: (4, 56040)
        //   c1  same shape                              -> whole chunk:         (56143, 56030)
        //   c2  dict 402 of 4,372, 5 selected pages = 1,985
        //       -> coverage 0.546, per-page: dict (112274, 402) plus ONE merged, padded run
        //          (114661, 1985) -- pages 5..9 are contiguous and the run already ends at the
        //          chunk end, so the peek adds nothing.
        EXPECT_EQ(ranges.size(), 4);

        // Each registered IORange contributes exactly 1 to some SharedBuffer's ref_count, so this sum
        // is the number of ranges init() registered: 10 page-index ranges (5 per row group, both
        // groups are filtered during init) plus the 4 page ranges of the prepared group. It was 28
        // when the page path emitted 18.
        EXPECT_EQ(shared_buffer->current_range_ref_sum(), 10 + static_cast<int64_t>(ranges.size()));

        // The second row group is not prepare yet

        size_t total_row_nums = 0;
        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            chunk->check_or_die();
            total_row_nums += chunk->num_rows();
        }
        EXPECT_EQ(total_row_nums, 10000);
    };

    test();
}

TEST_F(PageIndexTest, TestPageIndexNoPageFiltered) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());

    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    Utils::SlotDesc min_max_slots[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
            {""},
    };

    auto ctx = _create_file_c0_c1_c2_context(small_page_file);
    auto file = _create_file(small_page_file);
    ctx->format_scan_context.conjunct_ctxs_by_slot[0].clear();
    ctx->format_scan_context.conjunct_ctxs_by_slot[1].clear();
    ctx->format_scan_context.conjuncts.min_max_ctxs.clear();
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    std::vector<TExpr> t_conjuncts;
    // c0: 1->20000, c0 > 500
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 500, &t_conjuncts);
    // c1: 20000->1, c1 > 500
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, 500, &t_conjuncts);

    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                        &ctx->format_scan_context.conjuncts.min_max_ctxs);

    std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0]};
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0,
                                        &ctx->format_scan_context.conjunct_ctxs_by_slot[0]);

    std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[1]};
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1,
                                        &ctx->format_scan_context.conjunct_ctxs_by_slot[1]);

    auto shared_buffer = std::make_shared<SharedBufferedInputStream>(file->stream(), small_page_file,
                                                                     std::filesystem::file_size(small_page_file));
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(small_page_file), DataCacheOptions(),
                                                    shared_buffer.get());

    Status status = file_reader->init(&ctx->format_scan_context);
    ASSERT_TRUE(status.ok());

    // two row groups.
    EXPECT_EQ(file_reader->_row_group_readers.size(), 2);
    std::vector<SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;

    for (auto& r : file_reader->_row_group_readers) {
        r->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
    }

    // collect io of column index and offset index for active column,
    // and offset index for lazy column
    // and two group collect together. (2 + 2 + 1) * 2 = 10
    EXPECT_EQ(ranges.size(), 10);
    std::cout << "range ref sum:" << shared_buffer->current_range_ref_sum() << std::endl;

    ranges.clear();
    end_offset = 0;

    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);
    // only collect io of 1 chunk / column.
    // three columns, 1 * 3 = 3

    for (auto r : ranges) {
        std::cout << r.offset << " " << r.size << ' ' << r.is_active << std::endl;
    }
    EXPECT_EQ(ranges.size(), 3);

    EXPECT_EQ(shared_buffer->current_range_ref_sum(), 13);

    // The second row group is not prepare yet

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(total_row_nums, 19000);
}

// Test for parquet files with empty null_counts in ColumnIndex.
// This can happen with some parquet writers that don't populate null_counts.
// The bug was accessing column_index.null_counts[i] without bounds checking.
TEST_F(PageIndexTest, TestEmptyNullCountsInColumnIndex) {
    const std::string file_path = "./be/test/formats/parquet/test_data/empty_null_counts_test.parquet";

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true),
                         chunk->num_columns());

    Utils::SlotDesc slot_descs[] = {
            {"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {"name", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"value", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {""},
    };

    Utils::SlotDesc min_max_slots[] = {
            {"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), 0},
            {""},
    };

    auto ctx = _create_scan_context();
    auto file = _create_file(file_path);
    ctx->format_scan_context.conjunct_ctxs_by_slot[0].clear();
    ctx->format_scan_context.conjuncts.min_max_ctxs.clear();

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->format_scan_context.materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    _set_scan_range(ctx, _create_scan_range(file_path));
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    std::vector<TExpr> t_conjuncts;
    // id > 2
    // this triggers page index filtering
    ParquetUTBase::append_bigint_conjunct(TExprOpcode::GT, 0, 2, &t_conjuncts);

    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                        &ctx->format_scan_context.conjuncts.min_max_ctxs);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                        &ctx->format_scan_context.conjunct_ctxs_by_slot[0]);

    std::vector<ExprContext*> all_conjuncts;
    for (auto* expr : ctx->format_scan_context.conjuncts.min_max_ctxs) {
        all_conjuncts.push_back(expr);
    }
    for (auto* expr : ctx->format_scan_context.conjunct_ctxs_by_slot[0]) {
        all_conjuncts.push_back(expr);
    }
    ParquetUTBase::setup_conjuncts_manager(all_conjuncts, nullptr, tuple_desc, _runtime_state, ctx);

    auto shared_buffer = std::make_shared<SharedBufferedInputStream>(file->stream(), file_path,
                                                                     std::filesystem::file_size(file_path));
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(file_path),
                                         DataCacheOptions(), shared_buffer.get());

    Status status = file_reader->init(&ctx->format_scan_context);
    ASSERT_TRUE(status.ok());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_GT(total_row_nums, 0);
}

} // namespace starrocks::parquet
