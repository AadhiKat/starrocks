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

import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class RapManifestPredicateTest {
    private static final ColumnRefOperator COL = new ColumnRefOperator(1, IntegerType.BIGINT, "v", true);
    private static final ColumnRefOperator OTHER = new ColumnRefOperator(2, IntegerType.BIGINT, "other", true);

    private static ScalarOperator comparison(BinaryType type, long value) {
        return new BinaryPredicateOperator(type, COL, ConstantOperator.createBigint(value));
    }

    private static String manifest(String postings, String nulls) {
        return "{\"version\":2,\"key_type\":2,\"key_encoding\":\"hex\",\"table_uuid\":\"table\","
                + "\"snapshot_id\":1,\"column\":\"v\",\"field_id\":1,\"granularity_rows\":10,"
                + "\"files\":[{\"name\":\"gs/b/t/data/a\",\"size\":100,\"rows\":10},"
                + "{\"name\":\"gs/b/t/data/b\",\"size\":100,\"rows\":10}],"
                + "\"postings\":" + postings + ",\"null_postings\":" + nulls + "}";
    }

    private static String manifest() {
        return manifest("{\"8000000000000009\":[0],\"800000000000000a\":[1]}", "[0]");
    }

    private static DataFile file(String name, long size) {
        return DataFiles.builder(PartitionSpec.unpartitioned()).withPath("gs://b/t/data/" + name)
                .withFormat("PARQUET").withFileSizeInBytes(size).withRecordCount(10).build();
    }

    private static List<RapCoverage.Decision> decisions(String json, ScalarOperator predicate) {
        RapCoverage coverage = RapCoverage.fromJson(json, "table", 1, predicate);
        return List.of(coverage.decide(file("a", 100)), coverage.decide(file("b", 100)),
                coverage.decide(file("uncovered", 100)));
    }

    private static void expect(ScalarOperator predicate, RapCoverage.Decision a, RapCoverage.Decision b) {
        Assertions.assertEquals(List.of(a, b, RapCoverage.Decision.KEEP), decisions(manifest(), predicate));
    }

    @Test
    void numericOrderingBoundsAndReversedOperands() {
        expect(comparison(BinaryType.GT, 9), RapCoverage.Decision.DROP, RapCoverage.Decision.KEEP);
        expect(comparison(BinaryType.GE, 9), RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
        expect(comparison(BinaryType.LT, 10), RapCoverage.Decision.KEEP, RapCoverage.Decision.DROP);
        expect(comparison(BinaryType.LE, 10), RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
        expect(new BinaryPredicateOperator(BinaryType.LT, ConstantOperator.createBigint(9), COL),
                RapCoverage.Decision.DROP, RapCoverage.Decision.KEEP);
        expect(comparison(BinaryType.EQ, 100), RapCoverage.Decision.DROP, RapCoverage.Decision.DROP);
    }

    @Test
    void conjunctionIntersectsAndNeverNarrowsFromOr() {
        expect(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                comparison(BinaryType.GE, 9), comparison(BinaryType.LT, 10)),
                RapCoverage.Decision.KEEP, RapCoverage.Decision.DROP);
        expect(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                comparison(BinaryType.GT, 9), comparison(BinaryType.LT, 10)),
                RapCoverage.Decision.DROP, RapCoverage.Decision.DROP);
        expect(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                comparison(BinaryType.EQ, 9), new BinaryPredicateOperator(BinaryType.EQ, OTHER,
                        ConstantOperator.createBigint(4))), RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
    }

    @Test
    void inIsAtomicAndNullHasSeparatePostings() {
        expect(new InPredicateOperator(false, List.of(COL, ConstantOperator.createBigint(9),
                ConstantOperator.createBigint(10))), RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
        expect(new InPredicateOperator(false, List.of(COL, ConstantOperator.createBigint(9), OTHER)),
                RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
        expect(new InPredicateOperator(false, List.of(COL, ConstantOperator.createBigint(9),
                ConstantOperator.createNull(IntegerType.BIGINT))), RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
        expect(new IsNullPredicateOperator(COL), RapCoverage.Decision.KEEP, RapCoverage.Decision.DROP);
        expect(new IsNullPredicateOperator(true, COL), RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
        Assertions.assertEquals(List.of(RapCoverage.Decision.DROP, RapCoverage.Decision.DROP, RapCoverage.Decision.KEEP),
                decisions(manifest("{}", "[]"), new IsNullPredicateOperator(COL)));
    }

    @Test
    void wrongTypeAndMalformedUnusedPostingKeepEverything() {
        expect(new BinaryPredicateOperator(BinaryType.EQ, COL, ConstantOperator.createVarchar("9")),
                RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP);
        for (String postings : List.of("{\"09\":[0]}", "{\"8000000000000009\":[0.5]}",
                "{\"8000000000000009\":[4294967296]}", "{\"8000000000000009\":[0],\"INVALID\":[1]}",
                "{\"8000000000000009\":[0],\"8000000000000009\":[1]}")) {
            Assertions.assertEquals(List.of(RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP),
                    decisions(manifest(postings, "[]"), comparison(BinaryType.EQ, 9)));
        }
        Assertions.assertEquals(List.of(RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP),
                decisions(manifest("{}", "[2]"), comparison(BinaryType.EQ, 9)));
    }

    @Test
    void canonicalEncodingsAgreeWithBeLayout() {
        Assertions.assertEquals("0000000000000000", RapManifestPredicate.encodeLong(Long.MIN_VALUE));
        Assertions.assertEquals("7fffffffffffffff", RapManifestPredicate.encodeLong(-1));
        Assertions.assertEquals("8000000000000000", RapManifestPredicate.encodeLong(0));
        Assertions.assertEquals("ffffffffffffffff", RapManifestPredicate.encodeLong(Long.MAX_VALUE));
        Assertions.assertEquals("01", RapManifestPredicate.literal(ConstantOperator.createBoolean(true), 3));
        LocalDateTime epoch = LocalDateTime.of(1970, 1, 1, 0, 0);
        Assertions.assertEquals("8000000000253d8c", RapManifestPredicate.literal(ConstantOperator.createDate(epoch), 4));
        Assertions.assertEquals("a53d8c0000000001",
                RapManifestPredicate.literal(ConstantOperator.createDatetime(epoch.plusNanos(1000)), 5));
        Assertions.assertEquals("c3a9", RapManifestPredicate.literal(ConstantOperator.createVarchar("é"), 1));
    }

    @Test
    void identityMismatchRemainsUncovered() {
        RapCoverage coverage = RapCoverage.fromJson(manifest(), "table", 1, comparison(BinaryType.EQ, 100));
        Assertions.assertEquals(RapCoverage.Decision.KEEP, coverage.decide(file("a", 101)));
        Assertions.assertEquals(1, coverage.getIdentityMismatch());
    }

    @Test
    void legacyMixedInNeverUsesAPartialList() {
        ColumnRefOperator string = new ColumnRefOperator(1, VarcharType.VARCHAR, "model", true);
        Assertions.assertNull(RapCoverage.extractLiterals(new InPredicateOperator(false,
                List.of(string, ConstantOperator.createVarchar("first"), OTHER)), "model"));
    }

    @Test
    void nativeSchemaFieldIdentityCannotBeReused() {
        String oldDirectory = Config.rap_manifest_dir;
        try (InMemoryFileIO io = new InMemoryFileIO()) {
            Config.rap_manifest_dir = "memory://rap";
            for (int fieldId : List.of(1, 2)) {
                Schema schema = new Schema(Types.NestedField.optional(fieldId, "v", Types.LongType.get()));
                TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                        "memory://table", Map.of());
                // Creating a fresh table reassigns ids from one; model a subsequent schema change instead.
                if (fieldId != 1) {
                    metadata = TableMetadata.buildFrom(metadata).setCurrentSchema(schema, fieldId).build();
                }
                Assertions.assertEquals(fieldId, metadata.schema().findField("v").fieldId());
                BaseTable table = new BaseTable(new StaticTableOperations(metadata, io), "table");
                String json = manifest().replace("\"table_uuid\":\"table\"", "\"table_uuid\":\"" + metadata.uuid() + "\"");
                io.addFile(Config.rap_manifest_dir + "/" + metadata.uuid() + "/1.rapm.json",
                        json.getBytes(StandardCharsets.UTF_8));
                RapCoverage coverage = RapCoverage.load(table, Optional.of(1L), comparison(BinaryType.EQ, 100));
                Assertions.assertEquals(fieldId == 1, coverage.isActive());
                Assertions.assertEquals(fieldId == 1 ? RapCoverage.Decision.DROP : RapCoverage.Decision.KEEP,
                        coverage.decide(file("a", 100)));
            }
        } finally {
            Config.rap_manifest_dir = oldDirectory;
        }
    }

    @Test
    void pythonSerializedManifestIsReadWithoutTranslation() throws Exception {
        // Generated by rap_typed_manifest_build.build_manifest from reference-encoded RAPX v2 bytes.
        try (InputStream input = getClass().getResourceAsStream("/rap/typed-manifest-v2.json")) {
            Assertions.assertNotNull(input);
            String json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            Assertions.assertEquals(List.of(RapCoverage.Decision.DROP, RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP),
                    decisions(json, comparison(BinaryType.EQ, 10)));
            Assertions.assertEquals(List.of(RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP, RapCoverage.Decision.KEEP),
                    decisions(json, new IsNullPredicateOperator(COL)));
        }
    }
}
