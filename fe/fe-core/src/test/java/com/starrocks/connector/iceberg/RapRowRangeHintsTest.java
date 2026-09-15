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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TRowRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;

/**
 * RAP R7: plan-time row ranges. The frontend computes, from the manifest it already reads at planning, the row
 * ranges each COVERED file needs, and ships them on that file's scan range(s) as {@code selected_row_ranges}; the
 * backend then uses them and never opens that file's sidecar.
 *
 * <p>The safety frame these cases pin down:
 * <ul>
 *   <li>hints only NARROW inside a file the completeness rule {@code M ∪ (P − C)} already scheduled -- never add,
 *       remove or reorder a file, so every case asserts the scheduled file set alongside the hints;</li>
 *   <li>a covered file's hint is exactly what its sidecar would have answered, which is what lets the backend skip
 *       the sidecar;</li>
 *   <li>an uncovered file, an unusable or non-ranged manifest, a stale identity or a shape the range index cannot
 *       answer produces NO hint and leaves that file's scan exactly as it is today;</li>
 *   <li>positions are absolute within the FILE, so a split file carries the same list on every scan range.</li>
 * </ul>
 */
public class RapRowRangeHintsTest extends TableTestBase {
    private static final String UUID = "11111111-2222-3333-4444-555555555555";
    private static final long SNAP = 6366882456050597382L;

    // key_type 1 (STRING): a manifest key is the hex of the literal's UTF-8 bytes, the same encoding
    // RapIndex::encode_literal writes into the sidecar.
    private static final String KEY_V = "76"; // 'v'
    private static final String KEY_W = "77"; // 'w'

    private static DataFile file(String name, long size, long rows) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("gs://bucket/warehouse/db/t/data/" + name)
                .withFileSizeInBytes(size)
                .withRecordCount(rows)
                .withFormat("PARQUET")
                .build();
    }

    private static final DataFile F1 = file("f1.parquet", 1000, 100);
    private static final DataFile F2 = file("f2.parquet", 2000, 200);
    private static final DataFile F3 = file("f3.parquet", 3000, 300); // uncovered

    /**
     * The two covered files, each declaring the GRANULARITY its granule ordinals are read against. 10 rows per
     * granule, so F1 has ceil(100/10) = 10 granules and F2 has 20.
     */
    private static final String FILES =
            "\"files\": [{\"name\": \"gs/bucket/warehouse/db/t/data/f1.parquet\", \"size\": 1000, \"rows\": 100, "
                    + "\"granularity\": 10}, "
                    + "{\"name\": \"gs/bucket/warehouse/db/t/data/f2.parquet\", \"size\": 2000, \"rows\": 200, "
                    + "\"granularity\": 10}], ";

    /**
     * A v3 manifest over F1 (100 rows) and F2 (200 rows), with the postings and the GRANULE ORDINALS that describe
     * them: 'v' lives in F1 rows [0,20) and [40,60), which at granularity 10 is granules 0, 1, 4 and 5; 'w' lives
     * in F1 rows [50,70) (granules 5 and 6) and in all of F2 (granules 0-9 of the 20 F2 has); F2 also holds NULLs
     * in rows [100,200), granules 10-19. F3 is not named at all, so it is uncovered and always scanned.
     *
     * <p>Every derived range below is exactly the range the row-pair form of this manifest carried, because the
     * frontend coalesces adjacent granules and clips the last one to the file's rows.
     */
    private static String rangedManifest() {
        return rangedManifest("\"granules\": {"
                + "\"" + KEY_V + "\": {\"0\": [0, 1, 4, 5]}, "
                + "\"" + KEY_W + "\": {\"0\": [5, 6], \"1\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}}, "
                + "\"null_granules\": {\"1\": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]}");
    }

    /** The same manifest with the granules section replaced, so a single defect can be injected at a time. */
    private static String rangedManifest(String granulesSection) {
        return rangedManifest(FILES, granulesSection);
    }

    /** ...and with the files section replaced too, for the defects that live in a file's own `granularity`. */
    private static String rangedManifest(String filesSection, String granulesSection) {
        return "{\"version\": 3, \"key_type\": 1, \"key_encoding\": \"hex\", \"table_uuid\": \"" + UUID + "\", "
                + "\"snapshot_id\": " + SNAP + ", \"column\": \"model\", \"field_id\": 15, "
                + "\"granularity_rows\": 20000, "
                + filesSection
                + "\"postings\": {\"" + KEY_V + "\": [0], \"" + KEY_W + "\": [0, 1]}, "
                + "\"null_postings\": [1]"
                + (granulesSection == null ? "" : ", " + granulesSection) + "}";
    }

    private static ScalarOperator eq(String col, String lit) {
        return new BinaryPredicateOperator(BinaryType.EQ, new ColumnRefOperator(1, VARCHAR, col, true),
                ConstantOperator.createVarchar(lit));
    }

    private static ScalarOperator cmp(BinaryType type, String col, String lit) {
        return new BinaryPredicateOperator(type, new ColumnRefOperator(1, VARCHAR, col, true),
                ConstantOperator.createVarchar(lit));
    }

    private static ScalarOperator in(boolean notIn, String col, String... lits) {
        List<ScalarOperator> args = new ArrayList<>();
        args.add(new ColumnRefOperator(1, VARCHAR, col, true));
        for (String l : lits) {
            args.add(ConstantOperator.createVarchar(l));
        }
        return new InPredicateOperator(notIn, args);
    }

    private static ScalarOperator isNull(String col, boolean notNull) {
        return new IsNullPredicateOperator(notNull, new ColumnRefOperator(1, VARCHAR, col, true));
    }

    private static ScalarOperator and(ScalarOperator... children) {
        return new CompoundPredicateOperator(CompoundType.AND, children);
    }

    /** Rendered ranges of one file, or "none" -- so a failure prints what was hinted, not just that it differed. */
    private static String hintOf(RapCoverage c, DataFile f) {
        long[][] ranges = c.rowRangesFor(f);
        if (ranges == null) {
            return "none";
        }
        StringBuilder sb = new StringBuilder();
        for (long[] r : ranges) {
            sb.append(sb.length() == 0 ? "" : ",").append("[").append(r[0]).append(",").append(r[1]).append(")");
        }
        return sb.toString();
    }

    /** The files this coverage would schedule, by basename, so the completeness rule is asserted beside the hints. */
    private static List<String> scheduled(RapCoverage c) {
        List<String> kept = new ArrayList<>();
        for (DataFile f : Arrays.asList(F1, F2, F3)) {
            if (c.decide(f) == RapCoverage.Decision.KEEP) {
                String loc = f.location();
                kept.add(loc.substring(loc.lastIndexOf('/') + 1));
            }
        }
        return kept;
    }

    // -------------------------------------------------------------------------------------------------------
    // 1. A covered file gets exactly the ranges its sidecar would have answered; an uncovered file gets none.
    // -------------------------------------------------------------------------------------------------------

    @Test
    public void testCoveredFileGetsExactlyThePostingRanges() {
        RapCoverage c = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.isActive());
        Assertions.assertTrue(c.hasRowRangeHints(), c.getHintReason());
        // F1 holds 'v' in two bands; F2 does not hold it at all and is eliminated; F3 is uncovered and kept whole.
        Assertions.assertEquals("[0,20),[40,60)", hintOf(c, F1));
        Assertions.assertEquals("none", hintOf(c, F3), "an uncovered file is scanned as before");
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(c));
        Assertions.assertEquals(1, c.getDropped());
        Assertions.assertEquals(1, c.getHinted());
        Assertions.assertEquals(2, c.getHintedRanges());
    }

    @Test
    public void testInUnionsRangesAcrossLiterals() {
        RapCoverage c = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, in(false, "model", "v", "w"));
        Assertions.assertTrue(c.hasRowRangeHints(), c.getHintReason());
        // 'v' -> [0,20),[40,60) and 'w' -> [50,70) in F1: the union is merged, never left overlapping, because the
        // backend refuses an unordered or overlapping list.
        Assertions.assertEquals("[0,20),[40,70)", hintOf(c, F1));
        Assertions.assertEquals("[0,100)", hintOf(c, F2));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scheduled(c));
    }

    @Test
    public void testConjunctionIntersectsRangesWithinAFile() {
        RapCoverage c = RapCoverage.fromJson(rangedManifest(), UUID, SNAP,
                and(eq("model", "v"), eq("model", "w")));
        Assertions.assertTrue(c.hasRowRangeHints(), c.getHintReason());
        // Only rows that can hold BOTH: [40,60) ∩ [50,70) = [50,60). F2 holds no 'v' and is eliminated.
        Assertions.assertEquals("[50,60)", hintOf(c, F1));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(c));
    }

    @Test
    public void testNullAndNotNullCarryTheirOwnRanges() {
        RapCoverage isNullCoverage = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, isNull("model", false));
        Assertions.assertTrue(isNullCoverage.hasRowRangeHints(), isNullCoverage.getHintReason());
        Assertions.assertEquals("none", hintOf(isNullCoverage, F1));
        Assertions.assertEquals("[100,200)", hintOf(isNullCoverage, F2));
        Assertions.assertEquals(Arrays.asList("f2.parquet", "f3.parquet"), scheduled(isNullCoverage));

        RapCoverage notNull = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, isNull("model", true));
        Assertions.assertTrue(notNull.hasRowRangeHints(), notNull.getHintReason());
        Assertions.assertEquals("[0,20),[40,70)", hintOf(notNull, F1));
        Assertions.assertEquals("[0,100)", hintOf(notNull, F2));
    }

    @Test
    public void testOrderedComparisonCarriesTheTailOfThePostings() {
        RapCoverage c = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, cmp(BinaryType.GT, "model", "v"));
        Assertions.assertTrue(c.hasRowRangeHints(), c.getHintReason());
        Assertions.assertEquals("[50,70)", hintOf(c, F1), "> 'v' selects only the 'w' band");
        Assertions.assertEquals("[0,100)", hintOf(c, F2));
    }

    // -------------------------------------------------------------------------------------------------------
    // 2. No hints: unusable, non-ranged, stale, disabled, or a shape the range index cannot answer.
    //    Each case asserts WHICH reason was raised, and that elimination is unchanged by the hint failure.
    // -------------------------------------------------------------------------------------------------------

    @Test
    public void testManifestWithoutRangesHintsNothingAndStillEliminates() {
        RapCoverage c = RapCoverage.fromJson(rangedManifest(null), UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.isActive());
        Assertions.assertFalse(c.hasRowRangeHints());
        Assertions.assertEquals("v3 manifest without a granules object", c.getHintReason());
        Assertions.assertEquals("none", hintOf(c, F1));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(c),
                "a hint defect must not change which files are scheduled");
    }

    @Test
    public void testAV2ManifestHintsNothing() {
        String v2 = rangedManifest().replace("\"version\": 3", "\"version\": 2");
        RapCoverage c = RapCoverage.fromJson(v2, UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.isActive(), "v2 keeps eliminating exactly as it does today");
        Assertions.assertFalse(c.hasRowRangeHints());
        Assertions.assertEquals("none", hintOf(c, F1));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(c));
    }

    @Test
    public void testMalformedRangesHintNothingAndNameTheDefect() {
        record Case(String section, String reason) {
        }
        String w = "\"" + KEY_W + "\": {\"0\": [5, 6], \"1\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}}, ";
        String nulls = "\"null_granules\": {\"1\": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]}";
        List<Case> cases = List.of(
                // the twelve shapes the row-pair form pinned, each now expressed as ordinals
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [0, 1, 4, 5]}}, " + nulls,
                        "granules and postings name different values"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [0, 1, 4, 5]}, "
                        + "\"" + KEY_W + "\": {\"0\": [5, 6]}}, " + nulls,
                        "granules and postings name different files for a value"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [10]}, " + w + nulls,
                        "a granule ordinal is outside the file's granules"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [4, 0]}, " + w + nulls,
                        "granule ordinals are unordered or repeated"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [0, 0]}, " + w + nulls,
                        "granule ordinals are unordered or repeated"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [-1]}, " + w + nulls,
                        "a granule ordinal is outside the file's granules"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [0.5]}, " + w + nulls,
                        "a granule ordinal is not an exact integer"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [0, 1, 4, 5]}, " + w
                        + "\"null_granules\": {\"0\": [0]}",
                        "granules and postings name different files for a value"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [0, 1, 4, 5]}, " + w.substring(0, w.length() - 2),
                        "v3 manifest without a null_granules object"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"7\": [0]}, " + w + nulls,
                        "granule file ordinal outside the manifest files"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": []}, " + w + nulls,
                        "a file's granule list is absent or empty"),
                new Case("\"granules\": {\"" + KEY_V + "\": {\"0\": [\"0\"]}, " + w + nulls,
                        "a granule ordinal is not a number"),
                // ...and the two structural shapes a section rename does not change
                new Case("\"granules\": {\"" + KEY_V + "\": {\"f1\": [0]}, " + w + nulls,
                        "granule file ordinal is not an integer"),
                new Case("\"granules\": {\"" + KEY_V + "\": [], " + w + nulls,
                        "a posting value has no granules object"));
        for (Case one : cases) {
            RapCoverage c = RapCoverage.fromJson(rangedManifest(one.section()), UUID, SNAP, eq("model", "v"));
            Assertions.assertTrue(c.isActive(), one.reason());
            Assertions.assertFalse(c.hasRowRangeHints(), "expected no hints for: " + one.reason());
            Assertions.assertEquals(one.reason(), c.getHintReason());
            Assertions.assertEquals("none", hintOf(c, F1));
            // The whole point: a broken granules section costs the narrowing, never the completeness rule.
            Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(c));
        }
    }

    /**
     * The granularity is what turns an ordinal back into rows, so a file that does not declare a usable one
     * cannot be hinted at all -- and, like every other hint defect, that costs the narrowing and nothing else.
     */
    @Test
    public void testMalformedGranularityHintsNothingAndNamesTheDefect() {
        String head = "\"files\": [{\"name\": \"gs/bucket/warehouse/db/t/data/f1.parquet\", \"size\": 1000, "
                + "\"rows\": 100, ";
        String tail = "{\"name\": \"gs/bucket/warehouse/db/t/data/f2.parquet\", \"size\": 2000, \"rows\": 200, "
                + "\"granularity\": 10}], ";
        List<String> files = List.of(
                head + "\"granularity\": 10}, " + tail.replace("\"granularity\": 10}", "\"granularity\": 0}"),
                head.substring(0, head.length() - 2) + "}, " + tail,           // no granularity at all
                head + "\"granularity\": 10.5}, " + tail,
                head + "\"granularity\": \"10\"}, " + tail,
                head + "\"granularity\": -10}, " + tail);
        for (String filesSection : files) {
            RapCoverage c = RapCoverage.fromJson(rangedManifest(filesSection,
                    "\"granules\": {\"" + KEY_V + "\": {\"0\": [0, 1, 4, 5]}, "
                            + "\"" + KEY_W + "\": {\"0\": [5, 6], \"1\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}}, "
                            + "\"null_granules\": {\"1\": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]}"),
                    UUID, SNAP, eq("model", "v"));
            Assertions.assertTrue(c.isActive(), filesSection);
            Assertions.assertFalse(c.hasRowRangeHints(), filesSection);
            Assertions.assertEquals("a file declares no usable granularity", c.getHintReason(), filesSection);
            Assertions.assertEquals("none", hintOf(c, F1));
            Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(c));
        }
    }

    /**
     * The two properties the ordinal form rests on: the file's LAST granule is short -- clipped to its row count,
     * never running past it -- and adjacent granules are one range, not two that touch. Both are what make the
     * derived list identical to the one the sidecar holds.
     */
    @Test
    public void testTheLastGranuleIsShortAndAdjacentGranulesAreOneRange() {
        // granularity 30: F1 has ceil(100/30) = 4 granules, the last being [90,100); F2 has 7, the last [180,200).
        String files = "\"files\": [{\"name\": \"gs/bucket/warehouse/db/t/data/f1.parquet\", \"size\": 1000, "
                + "\"rows\": 100, \"granularity\": 30}, "
                + "{\"name\": \"gs/bucket/warehouse/db/t/data/f2.parquet\", \"size\": 2000, \"rows\": 200, "
                + "\"granularity\": 30}], ";
        String rest = "\"" + KEY_W + "\": {\"0\": [0], \"1\": [0]}}, \"null_granules\": {\"1\": [6]}";

        RapCoverage c = RapCoverage.fromJson(
                rangedManifest(files, "\"granules\": {\"" + KEY_V + "\": {\"0\": [0, 2, 3]}, " + rest),
                UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.hasRowRangeHints(), c.getHintReason());
        Assertions.assertEquals("[0,30),[60,100)", hintOf(c, F1),
                "granules 2 and 3 are adjacent, so they are ONE range, and 3 ends at the file's 100th row");
        Assertions.assertEquals(2, c.getHintedRanges());

        // The NULL posting's granule 6 of F2 is the short one: [180,200), not [180,210).
        RapCoverage nulls = RapCoverage.fromJson(
                rangedManifest(files, "\"granules\": {\"" + KEY_V + "\": {\"0\": [0]}, " + rest),
                UUID, SNAP, isNull("model", false));
        Assertions.assertTrue(nulls.hasRowRangeHints(), nulls.getHintReason());
        Assertions.assertEquals("[180,200)", hintOf(nulls, F2));

        // One past the last granule is refused -- where the row-pair form said "past the file's row count".
        RapCoverage over = RapCoverage.fromJson(
                rangedManifest(files, "\"granules\": {\"" + KEY_V + "\": {\"0\": [4]}, " + rest),
                UUID, SNAP, eq("model", "v"));
        Assertions.assertFalse(over.hasRowRangeHints());
        Assertions.assertEquals("a granule ordinal is outside the file's granules", over.getHintReason());
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(over));
    }

    @Test
    public void testPredicateShapeWithoutRangesHintsNothing() {
        // NOT IN is not narrowable: no elimination today, and no hints either.
        RapCoverage notIn = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, in(true, "model", "v"));
        Assertions.assertFalse(notIn.hasRowRangeHints());
        Assertions.assertEquals("no usable postings for this predicate", notIn.getHintReason());
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scheduled(notIn));

        // A predicate on another column: the manifest's column is not constrained, so nothing narrows.
        RapCoverage otherColumn = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, eq("other", "v"));
        Assertions.assertFalse(otherColumn.hasRowRangeHints());
        Assertions.assertEquals("no usable postings for this predicate", otherColumn.getHintReason());
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scheduled(otherColumn));
    }

    @Test
    public void testConfigCanTurnHintsOffWithoutTurningEliminationOff() {
        boolean saved = Config.rap_plan_row_range_hints;
        try {
            Config.rap_plan_row_range_hints = false;
            RapCoverage c = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, eq("model", "v"));
            Assertions.assertTrue(c.isActive());
            Assertions.assertFalse(c.hasRowRangeHints());
            Assertions.assertEquals("disabled by rap_plan_row_range_hints", c.getHintReason());
            Assertions.assertEquals("none", hintOf(c, F1));
            Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scheduled(c));
        } finally {
            Config.rap_plan_row_range_hints = saved;
        }
    }

    @Test
    public void testStaleFileIdentityGetsNoHint() {
        // The manifest says f1 is 1000 bytes / 100 rows. A file rewritten under the same name is UNCOVERED by the
        // completeness rule -- it is kept -- and it must not be narrowed by the old file's ranges either.
        DataFile rewritten = file("f1.parquet", 1001, 100);
        DataFile reRowed = file("f1.parquet", 1000, 101);
        RapCoverage c = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.hasRowRangeHints(), c.getHintReason());
        Assertions.assertEquals("[0,20),[40,60)", hintOf(c, F1), "the unchanged file is still hinted");
        Assertions.assertEquals("none", hintOf(c, rewritten));
        Assertions.assertEquals("none", hintOf(c, reRowed));
        Assertions.assertEquals(RapCoverage.Decision.KEEP, c.decide(rewritten));
        Assertions.assertEquals(RapCoverage.Decision.KEEP, c.decide(reRowed));
    }

    @Test
    public void testStaleSchemaIdentityDisablesTheManifestAndTheHints() throws Exception {
        // A same-name column with another field id is a different column: RapCoverage.load refuses the manifest
        // outright, so there is no elimination AND no hint.
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        long snap = mockedNativeTableA.currentSnapshot().snapshotId();
        String uuid = ((BaseTable) mockedNativeTableA).operations().current().uuid();
        File dir = Files.createTempDirectory("rap_manifest_ranged_").toFile();
        File tdir = new File(dir, uuid);
        Assertions.assertTrue(tdir.mkdirs());

        String good = tableRangedManifest(uuid, snap, 2);
        String stale = tableRangedManifest(uuid, snap, 999);
        String saved = Config.rap_manifest_dir;
        try {
            Config.rap_manifest_dir = dir.getAbsolutePath();
            Files.write(new File(tdir, snap + RapCoverage.SUFFIX).toPath(), good.getBytes(StandardCharsets.UTF_8));
            RapCoverage ok = RapCoverage.load(mockedNativeTableA, Optional.of(snap), eq("data", "v"));
            Assertions.assertTrue(ok.isActive());
            Assertions.assertTrue(ok.hasRowRangeHints(), ok.getHintReason());
            Assertions.assertNotNull(ok.rowRangesFor(FILE_A));

            Files.write(new File(tdir, snap + RapCoverage.SUFFIX).toPath(), stale.getBytes(StandardCharsets.UTF_8));
            RapCoverage off = RapCoverage.load(mockedNativeTableA, Optional.of(snap), eq("data", "v"));
            Assertions.assertFalse(off.isActive(), "a manifest bound to another field id must not activate");
            Assertions.assertFalse(off.hasRowRangeHints());
            Assertions.assertNull(off.rowRangesFor(FILE_A));
            Assertions.assertEquals(RapCoverage.Decision.KEEP, off.decide(FILE_A_1));
        } finally {
            Config.rap_manifest_dir = saved;
        }
    }

    /** A v3 manifest over the in-memory table's own files; {@code fieldId} is the identity under test. */
    private static String tableRangedManifest(String uuid, long snap, int fieldId) {
        return "{\"version\": 3, \"key_type\": 1, \"key_encoding\": \"hex\", \"table_uuid\": \"" + uuid + "\", "
                + "\"snapshot_id\": " + snap + ", \"column\": \"data\", \"field_id\": " + fieldId + ", "
                + "\"granularity_rows\": 20000, \"files\": ["
                + "{\"name\": \"" + RapCoverage.keyOf(FILE_A.location()) + "\", \"size\": " + FILE_A.fileSizeInBytes()
                + ", \"rows\": " + FILE_A.recordCount() + ", \"granularity\": 1}, "
                + "{\"name\": \"" + RapCoverage.keyOf(FILE_A_1.location()) + "\", \"size\": "
                + FILE_A_1.fileSizeInBytes() + ", \"rows\": " + FILE_A_1.recordCount() + ", \"granularity\": 1}], "
                + "\"postings\": {\"" + KEY_V + "\": [0]}, \"null_postings\": [], "
                // granularity 1: granule 0 of a 2-row file is row [0,1), the same hint the row-pair form carried
                + "\"granules\": {\"" + KEY_V + "\": {\"0\": [0]}}, \"null_granules\": {}}";
    }

    // -------------------------------------------------------------------------------------------------------
    // 3. The scan-range source actually ships them -- on every split of a file, file-relative.
    // -------------------------------------------------------------------------------------------------------

    @Test
    public void testScanRangeSourceShipsHintsOnCoveredFilesOnly() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        long snap = mockedNativeTableA.currentSnapshot().snapshotId();
        String uuid = ((BaseTable) mockedNativeTableA).operations().current().uuid();
        List<FileScanTask> tasks = new ArrayList<>();
        mockedNativeTableA.newScan().planFiles().forEach(tasks::add);
        Assertions.assertEquals(2, tasks.size());

        RapCoverage cov = RapCoverage.fromJson(tableRangedManifest(uuid, snap, 2), uuid, snap, eq("data", "v"));
        Assertions.assertTrue(cov.hasRowRangeHints(), cov.getHintReason());

        List<TScanRangeLocations> out = outputs(icebergTable(), tupleDescriptor(), tasks, cov);
        // FILE_A holds 'v' and is hinted; FILE_A_1 is covered with no posting and is not scheduled at all.
        Assertions.assertEquals(1, out.size());
        List<TRowRange> hints = out.get(0).getScan_range().getHdfs_scan_range().getSelected_row_ranges();
        Assertions.assertNotNull(hints);
        Assertions.assertEquals(1, hints.size());
        Assertions.assertTrue(hints.get(0).isSetStart_row() && hints.get(0).isSetEnd_row(),
                "both bounds must be on the wire: the backend refuses a list with a missing bound");
        Assertions.assertEquals(0, hints.get(0).getStart_row());
        Assertions.assertEquals(1, hints.get(0).getEnd_row());

        // Without a coverage nothing is hinted and nothing is eliminated -- byte-for-byte today's plan.
        List<TScanRangeLocations> plain = outputs(icebergTable(), tupleDescriptor(), tasks, null);
        Assertions.assertEquals(2, plain.size());
        for (TScanRangeLocations one : plain) {
            Assertions.assertFalse(one.getScan_range().getHdfs_scan_range().isSetSelected_row_ranges());
        }
    }

    @Test
    public void testUncoveredFileShipsNoHint() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_2).commit();
        long snap = mockedNativeTableA.currentSnapshot().snapshotId();
        String uuid = ((BaseTable) mockedNativeTableA).operations().current().uuid();
        List<FileScanTask> tasks = new ArrayList<>();
        mockedNativeTableA.newScan().planFiles().forEach(tasks::add);

        // The manifest names FILE_A and FILE_A_1; FILE_A_2 is in the plan but not in the manifest.
        RapCoverage cov = RapCoverage.fromJson(tableRangedManifest(uuid, snap, 2), uuid, snap, eq("data", "v"));
        List<TScanRangeLocations> out = outputs(icebergTable(), tupleDescriptor(), tasks, cov);
        Assertions.assertEquals(2, out.size(), "the uncovered file is always scheduled (P - C)");
        int hinted = 0;
        for (TScanRangeLocations one : out) {
            if (one.getScan_range().getHdfs_scan_range().isSetSelected_row_ranges()) {
                hinted++;
                Assertions.assertTrue(one.getScan_range().getHdfs_scan_range().getFull_path().endsWith("data-a.parquet"));
            }
        }
        Assertions.assertEquals(1, hinted, "exactly the covered, matching file carries a hint");
    }

    @Test
    public void testRangesAreFileRelativeAndRepeatedOnEverySplit() throws Exception {
        // A file big enough to split into several scan ranges. The hint is absolute within the FILE, so every split
        // carries the identical list and the backend intersects it with the row groups that split actually reads.
        DataFile big = DataFiles.builder(SPEC_A)
                .withPath("/path/to/data-big.parquet")
                .withFileSizeInBytes(3000)
                .withPartitionPath("data_bucket=0")
                .withRecordCount(600)
                .build();
        mockedNativeTableA.newFastAppend().appendFile(big).commit();
        long snap = mockedNativeTableA.currentSnapshot().snapshotId();
        String uuid = ((BaseTable) mockedNativeTableA).operations().current().uuid();

        List<FileScanTask> splits = new ArrayList<>();
        for (FileScanTask task : mockedNativeTableA.newScan().planFiles()) {
            task.split(1000).forEach(splits::add);
        }
        Assertions.assertTrue(splits.size() > 1, "fixture must actually split; got " + splits.size());

        String manifest = "{\"version\": 3, \"key_type\": 1, \"key_encoding\": \"hex\", \"table_uuid\": \"" + uuid
                + "\", \"snapshot_id\": " + snap + ", \"column\": \"data\", \"field_id\": 2, "
                + "\"granularity_rows\": 20000, \"files\": [{\"name\": \"" + RapCoverage.keyOf(big.location())
                + "\", \"size\": 3000, \"rows\": 600, \"granularity\": 20}], "
                + "\"postings\": {\"" + KEY_V + "\": [0]}, \"null_postings\": [], "
                // granularity 20 over 600 rows: granule 1 is [20,40), and 28 + 29 coalesce into [560,600)
                + "\"granules\": {\"" + KEY_V + "\": {\"0\": [1, 28, 29]}}, \"null_granules\": {}}";
        RapCoverage cov = RapCoverage.fromJson(manifest, uuid, snap, eq("data", "v"));
        Assertions.assertTrue(cov.hasRowRangeHints(), cov.getHintReason());

        List<TScanRangeLocations> out = outputs(icebergTable(), tupleDescriptor(), splits, cov);
        Assertions.assertEquals(splits.size(), out.size());
        List<Long> offsets = new ArrayList<>();
        for (TScanRangeLocations one : out) {
            offsets.add(one.getScan_range().getHdfs_scan_range().getOffset());
            List<TRowRange> hints = one.getScan_range().getHdfs_scan_range().getSelected_row_ranges();
            Assertions.assertNotNull(hints, "every split of a hinted file carries the hint");
            Assertions.assertEquals(2, hints.size());
            Assertions.assertEquals(20, hints.get(0).getStart_row());
            Assertions.assertEquals(40, hints.get(0).getEnd_row());
            Assertions.assertEquals(560, hints.get(1).getStart_row());
            Assertions.assertEquals(600, hints.get(1).getEnd_row());
        }
        Assertions.assertEquals(offsets.size(), offsets.stream().distinct().count(),
                "the splits really are different byte ranges of one file, yet the row hint is the same on each");
    }

    // -------------------------------------------------------------------------------------------------------
    // 4. The mirror invariant. matchingRanges() must select exactly the files matching() selects, for every
    //    supported shape -- that is what licenses shipping a hint for a file the elimination rule scheduled.
    //    RapCoverage also asserts it per plan, but that assertion cannot fire while the two agree, so THIS is
    //    the control: break either method and it goes red here.
    // -------------------------------------------------------------------------------------------------------

    @Test
    public void testMatchingAndMatchingRangesSelectTheSameFiles() {
        NavigableMap<String, Set<Integer>> postings = new TreeMap<>();
        postings.put(KEY_V, Set.of(0));
        postings.put(KEY_W, Set.of(0, 1));
        NavigableMap<String, Map<Integer, long[][]>> ranged = new TreeMap<>();
        ranged.put(KEY_V, Map.of(0, ranges(0, 20, 40, 60)));
        ranged.put(KEY_W, Map.of(0, ranges(50, 70), 1, ranges(0, 100)));
        Set<Integer> nullFiles = Set.of(1);
        Map<Integer, long[][]> nullRanges = Map.of(1, ranges(100, 200));

        List<ScalarOperator> shapes = List.of(
                eq("model", "v"),
                eq("model", "w"),
                eq("model", "absent"),
                in(false, "model", "v", "w"),
                in(true, "model", "v"),
                isNull("model", false),
                isNull("model", true),
                cmp(BinaryType.GT, "model", "v"),
                cmp(BinaryType.GE, "model", "v"),
                cmp(BinaryType.LT, "model", "w"),
                cmp(BinaryType.LE, "model", "w"),
                and(eq("model", "v"), eq("model", "w")),
                and(eq("model", "v"), eq("other", "x")),
                and(eq("other", "x"), eq("other", "y")),
                new CompoundPredicateOperator(CompoundType.OR, eq("model", "v"), eq("model", "w")));

        for (ScalarOperator shape : shapes) {
            Set<Integer> files = RapManifestPredicate.matching(shape, "model", 1, postings, nullFiles);
            Map<Integer, long[][]> withRanges =
                    RapManifestPredicate.matchingRanges(shape, "model", 1, ranged, nullRanges);
            if (files == null) {
                Assertions.assertNull(withRanges, "unsupported must stay unsupported for: " + shape);
            } else {
                Assertions.assertNotNull(withRanges, "supported must stay supported for: " + shape);
                Assertions.assertEquals(files, withRanges.keySet(), "file sets must agree for: " + shape);
            }
        }
    }

    @Test
    public void testRangeAlgebraMergesAndIntersects() {
        Assertions.assertArrayEquals(ranges(0, 20, 40, 70),
                RapManifestPredicate.mergeRanges(List.of(new long[] {40, 60}, new long[] {0, 10},
                        new long[] {50, 70}, new long[] {10, 20})));
        Assertions.assertEquals(0, RapManifestPredicate.mergeRanges(List.of()).length);
        Assertions.assertArrayEquals(ranges(50, 60),
                RapManifestPredicate.intersectRanges(ranges(0, 20, 40, 60), ranges(50, 70)));
        Assertions.assertEquals(0, RapManifestPredicate.intersectRanges(ranges(0, 20), ranges(20, 40)).length,
                "half-open intervals that only touch intersect to nothing");
    }

    // -------------------------------------------------------------------------------------------------------
    // 5. K2 / R9: the fallback says WHY. The reason is decided at LOAD time, so unlike the four counters on the
    //    same line it is real when EXPLAIN renders. The last case here is the one that protects the runners:
    //    the fields they parse must still be where they were, because the new ones are appended.
    // -------------------------------------------------------------------------------------------------------

    @Test
    public void testEveryFallbackNamesItsReason() throws Exception {
        Assertions.assertEquals(RapCoverage.REASON_DISABLED,
                RapCoverage.load("", mockedNativeTableA.io(), "u", Optional.of(SNAP), eq("model", "v")).getReason());
        Assertions.assertEquals(RapCoverage.REASON_NO_SNAPSHOT,
                RapCoverage.load("/tmp", mockedNativeTableA.io(), "u", Optional.empty(), eq("model", "v")).getReason());
        Assertions.assertEquals(RapCoverage.REASON_NO_TABLE,
                RapCoverage.load(null, Optional.of(SNAP), eq("model", "v")).getReason());
        Assertions.assertEquals(RapCoverage.REASON_NO_TABLE_UUID,
                RapCoverage.load("/tmp", mockedNativeTableA.io(), "", Optional.of(SNAP), eq("model", "v")).getReason());
        Assertions.assertEquals(RapCoverage.REASON_UNREADABLE,
                RapCoverage.load("/tmp", new ThrowingFileIO(), "u", Optional.of(SNAP), eq("model", "v")).getReason());

        // absent: a real directory through the table's own FileIO, with no manifest in it
        File dir = Files.createTempDirectory("rap_manifest_reason_").toFile();
        Assertions.assertEquals(RapCoverage.REASON_ABSENT, RapCoverage
                .load(dir.getAbsolutePath(), mockedNativeTableA.io(), UUID, Optional.of(SNAP), eq("model", "v"))
                .getReason());

        // oversize: the same manifest, read under a byte ceiling below its length -- never parsed in part
        File tdir = new File(dir, UUID);
        Assertions.assertTrue(tdir.mkdirs());
        Files.write(new File(tdir, SNAP + RapCoverage.SUFFIX).toPath(),
                rangedManifest().getBytes(StandardCharsets.UTF_8));
        long savedMax = Config.rap_manifest_max_bytes;
        try {
            Config.rap_manifest_max_bytes = 8;
            Assertions.assertEquals(RapCoverage.REASON_OVERSIZE, RapCoverage
                    .load(dir.getAbsolutePath(), mockedNativeTableA.io(), UUID, Optional.of(SNAP), eq("model", "v"))
                    .getReason());
        } finally {
            Config.rap_manifest_max_bytes = savedMax;
        }
        Assertions.assertEquals(RapCoverage.REASON_OK, RapCoverage
                .load(dir.getAbsolutePath(), mockedNativeTableA.io(), UUID, Optional.of(SNAP), eq("model", "v"))
                .getReason());

        // parse-time refusals
        Assertions.assertEquals(RapCoverage.REASON_UNSUPPORTED_VERSION, RapCoverage
                .fromJson(rangedManifest().replace("\"version\": 3", "\"version\": 9"), UUID, SNAP, eq("model", "v"))
                .getReason());
        Assertions.assertEquals(RapCoverage.REASON_OTHER_SNAPSHOT,
                RapCoverage.fromJson(rangedManifest(), UUID, SNAP + 1, eq("model", "v")).getReason());
        Assertions.assertEquals(RapCoverage.REASON_OTHER_TABLE,
                RapCoverage.fromJson(rangedManifest(), "another-uuid", SNAP, eq("model", "v")).getReason());
        Assertions.assertEquals(RapCoverage.REASON_MALFORMED, RapCoverage
                .fromJson(rangedManifest().replace("[0, 1]", "[0, 7]"), UUID, SNAP, eq("model", "v")).getReason());
        Assertions.assertEquals(RapCoverage.REASON_MALFORMED,
                RapCoverage.fromJson("{not json", UUID, SNAP, eq("model", "v")).getReason());

        // active, but not useful -- and the two cases are told apart
        Assertions.assertEquals(RapCoverage.REASON_PREDICATE_NOT_BOUND,
                RapCoverage.fromJson(rangedManifest(), UUID, SNAP, eq("other", "v")).getReason());
        // A v1 manifest that lists its files and carries no postings: M = C, nothing eliminated, backend narrows.
        // (A TYPED manifest without postings is malformed, not "no postings", and is refused above.)
        String noPostings = "{\"version\": 1, \"table_uuid\": \"" + UUID + "\", \"snapshot_id\": " + SNAP + ", "
                + "\"column\": \"model\", \"field_id\": 15, \"granularity_rows\": 20000, "
                + "\"files\": [{\"name\": \"gs/bucket/warehouse/db/t/data/f1.parquet\", \"size\": 1000, "
                + "\"rows\": 100}]}";
        RapCoverage noPost = RapCoverage.fromJson(noPostings, UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(noPost.isActive());
        Assertions.assertEquals(RapCoverage.REASON_NO_POSTINGS, noPost.getReason());
        Assertions.assertEquals(RapCoverage.Decision.KEEP, noPost.decide(F1));
    }

    @Test
    public void testStaleColumnIdentityNamesItsReason() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        long snap = mockedNativeTableA.currentSnapshot().snapshotId();
        String uuid = ((BaseTable) mockedNativeTableA).operations().current().uuid();
        File dir = Files.createTempDirectory("rap_manifest_stale_").toFile();
        File tdir = new File(dir, uuid);
        Assertions.assertTrue(tdir.mkdirs());
        Files.write(new File(tdir, snap + RapCoverage.SUFFIX).toPath(),
                tableRangedManifest(uuid, snap, 999).getBytes(StandardCharsets.UTF_8));
        String saved = Config.rap_manifest_dir;
        try {
            Config.rap_manifest_dir = dir.getAbsolutePath();
            RapCoverage c = RapCoverage.load(mockedNativeTableA, Optional.of(snap), eq("data", "v"));
            Assertions.assertFalse(c.isActive());
            Assertions.assertEquals(RapCoverage.REASON_STALE_COLUMN, c.getReason());
            Assertions.assertTrue(c.explain().contains("reason=stale_column_identity"));
        } finally {
            Config.rap_manifest_dir = saved;
        }
    }

    @Test
    public void testExplainCarriesTheReasonAndStillParsesForTheRunners() {
        RapCoverage on = RapCoverage.fromJson(rangedManifest(), UUID, SNAP, eq("model", "v"));
        Assertions.assertEquals("RAP MANIFEST: active snapshot=" + SNAP + " column=model postings=yes consulted=0 "
                + "covered=0 identity_mismatch=0 dropped=0 reason=ok hints=yes cache=none", on.explain());

        RapCoverage off = RapCoverage.disabled(SNAP, RapCoverage.REASON_OVERSIZE);
        Assertions.assertEquals("RAP MANIFEST: off snapshot=" + SNAP + " column= postings=no consulted=0 covered=0 "
                + "identity_mismatch=0 dropped=0 reason=oversize hints=no cache=none", off.explain());

        // The deployed runners parse this line with exactly this pattern (harness/deployed_check_fe.py). The new
        // fields are appended, so it must still match -- including the empty `column=` of a disabled coverage.
        Pattern runnerPattern = Pattern.compile("RAP MANIFEST: (\\w+) snapshot=(-?\\d+) column=(\\S*) postings=(\\w+) "
                + "consulted=(\\d+) covered=(\\d+) identity_mismatch=(\\d+) dropped=(\\d+)");
        for (RapCoverage c : List.of(on, off)) {
            Matcher m = runnerPattern.matcher(c.explain());
            Assertions.assertTrue(m.find(), "the runners' pattern no longer matches: " + c.explain());
            Assertions.assertEquals(8, m.groupCount());
        }
    }

    /** A FileIO that fails on every open, so the loader's unreadable path can be reached without a filesystem trick. */
    private static final class ThrowingFileIO implements org.apache.iceberg.io.FileIO {
        @Override
        public org.apache.iceberg.io.InputFile newInputFile(String path) {
            throw new RuntimeException("no");
        }

        @Override
        public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
            throw new RuntimeException("no");
        }

        @Override
        public void deleteFile(String path) {
            throw new RuntimeException("no");
        }
    }

    /** Flat [start, end) pairs as a range list, so the fixtures read as numbers rather than nested braces. */
    private static long[][] ranges(long... bounds) {
        long[][] out = new long[bounds.length / 2][];
        for (int i = 0; i < out.length; i++) {
            out[i] = new long[] {bounds[2 * i], bounds[2 * i + 1]};
        }
        return out;
    }

    private IcebergTable icebergTable() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("data", VARCHAR));
        return new IcebergTable(1, "iceberg_table", "iceberg_catalog", "resource", "db", "table", "",
                schema, mockedNativeTableA, Maps.newHashMap());
    }

    private static TupleDescriptor tupleDescriptor() {
        TupleDescriptor td = new TupleDescriptor(new TupleId(7));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), td);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), td);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        td.addSlot(idSlot);
        td.addSlot(dataSlot);
        return td;
    }

    private static List<TScanRangeLocations> outputs(IcebergTable table, TupleDescriptor td, List<FileScanTask> tasks,
                                                     RapCoverage cov) {
        List<RemoteFileInfo> infos = new ArrayList<>();
        for (FileScanTask t : tasks) {
            infos.add(new IcebergRemoteFileInfo(t));
        }
        IcebergConnectorScanRangeSource source = new IcebergConnectorScanRangeSource(table,
                new RemoteFileInfoDefaultSource(infos), IcebergMORParams.EMPTY, td, Optional.empty(),
                PartitionIdGenerator.of(), false, false);
        source.setRapCoverage(cov);
        return source.getSourceOutputs(100);
    }
}
