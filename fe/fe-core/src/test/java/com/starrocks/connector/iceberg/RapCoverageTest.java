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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
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
import java.util.Optional;

import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;

/**
 * RAP / lake-index slice 2b: the completeness rule M ∪ (P − C), pre-registered in slice-2b.md §4.
 * Files: f1, f2 covered; f3 uncovered. Postings: v -> [f1], w -> [f2].
 * Extends TableTestBase for the real-loader and scan-source hookup cases (astra CX-32), which use the
 * base's in-memory Iceberg table, its FileIO and its data files.
 */
public class RapCoverageTest extends TableTestBase {
    private static final String UUID = "11111111-2222-3333-4444-555555555555";
    private static final long SNAP = 6366882456050597382L;

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
    private static final DataFile F3 = file("f3.parquet", 3000, 300);

    private static String manifest(long snap, String uuid, boolean postings, long f2Size) {
        String base = "{\"version\": 1, \"table_uuid\": \"" + uuid + "\", \"snapshot_id\": " + snap + ", "
                + "\"column\": \"model\", \"field_id\": 15, \"granularity_rows\": 20000, "
                + "\"files\": [{\"name\": \"f1.parquet\", \"size\": 1000, \"rows\": 100}, "
                + "{\"name\": \"f2.parquet\", \"size\": " + f2Size + ", \"rows\": 200}]";
        if (postings) {
            base += ", \"postings\": {\"v\": [0], \"w\": [1]}";
        }
        return base + "}";
    }

    private static ScalarOperator eq(String col, String lit) {
        return new BinaryPredicateOperator(BinaryType.EQ, new ColumnRefOperator(1, VARCHAR, col, true),
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

    private static List<String> scanSet(RapCoverage c) {
        List<String> kept = new ArrayList<>();
        for (DataFile f : Arrays.asList(F1, F2, F3)) {
            if (c.decide(f) == RapCoverage.Decision.KEEP) {
                String loc = f.location();
                kept.add(loc.substring(loc.lastIndexOf('/') + 1));
            }
        }
        return kept;
    }

    @Test
    public void testEliminationKeepsUncovered() { // case a
        RapCoverage c = RapCoverage.fromJson(manifest(SNAP, UUID, true, 2000), UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.isActive());
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"), scanSet(c));
        Assertions.assertEquals(3, c.getConsulted());
        Assertions.assertEquals(2, c.getCovered());
        Assertions.assertEquals(1, c.getDropped());
        Assertions.assertEquals(0, c.getIdentityMismatch());
    }

    @Test
    public void testInUnionsPostings() { // case b
        RapCoverage c = RapCoverage.fromJson(manifest(SNAP, UUID, true, 2000), UUID, SNAP, in(false, "model", "v", "w"));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c));
        Assertions.assertEquals(0, c.getDropped());
    }

    @Test
    public void testNoPostingsMeansMEqualsC() { // case c
        RapCoverage c = RapCoverage.fromJson(manifest(SNAP, UUID, false, 2000), UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.isActive());
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c));
        Assertions.assertEquals(2, c.getCovered());
        Assertions.assertEquals(0, c.getDropped());
    }

    @Test
    public void testIdentityMismatchIsUncovered() { // case d
        RapCoverage c = RapCoverage.fromJson(manifest(SNAP, UUID, true, 2001), UUID, SNAP, eq("model", "v"));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c));
        Assertions.assertEquals(1, c.getIdentityMismatch());
        Assertions.assertEquals(1, c.getCovered());
        Assertions.assertEquals(0, c.getDropped());
    }

    @Test
    public void testOtherSnapshotIsIgnored() { // case e
        RapCoverage c = RapCoverage.fromJson(manifest(SNAP + 1, UUID, true, 2000), UUID, SNAP, eq("model", "v"));
        Assertions.assertFalse(c.isActive());
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c));
        Assertions.assertEquals(0, c.getCovered());
    }

    @Test
    public void testMalformedOrUnknownVersionKeepsAll() { // case f
        for (String bad : new String[] {"", "{", "{\"version\": 2, \"snapshot_id\": " + SNAP + "}",
                "{\"version\": 1, \"snapshot_id\": " + SNAP + ", \"column\": \"model\"}"}) {
            RapCoverage c = RapCoverage.fromJson(bad, UUID, SNAP, eq("model", "v"));
            Assertions.assertFalse(c.isActive(), bad);
            Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c), bad);
            Assertions.assertEquals(3, c.getConsulted());
            Assertions.assertEquals(0, c.getCovered());
        }
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(RapCoverage.disabled(SNAP)));
    }

    @Test
    public void testPredicateShapesThatEliminateNothing() { // case g
        ScalarOperator otherCol = eq("brand", "v");
        ScalarOperator gt = new BinaryPredicateOperator(BinaryType.GT, new ColumnRefOperator(1, VARCHAR, "model", true),
                ConstantOperator.createVarchar("v"));
        ScalarOperator notIn = in(true, "model", "v");
        for (ScalarOperator p : new ScalarOperator[] {otherCol, gt, notIn, null}) {
            RapCoverage c = RapCoverage.fromJson(manifest(SNAP, UUID, true, 2000), UUID, SNAP, p);
            Assertions.assertTrue(c.isActive());
            Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c));
            Assertions.assertEquals(0, c.getDropped());
        }
        // EQ under an AND is found; EQ under an OR is not used
        ScalarOperator and = new CompoundPredicateOperator(CompoundType.AND, eq("brand", "x"), eq("model", "v"));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f3.parquet"),
                scanSet(RapCoverage.fromJson(manifest(SNAP, UUID, true, 2000), UUID, SNAP, and)));
        ScalarOperator or = new CompoundPredicateOperator(CompoundType.OR, eq("brand", "x"), eq("model", "v"));
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"),
                scanSet(RapCoverage.fromJson(manifest(SNAP, UUID, true, 2000), UUID, SNAP, or)));
        // a literal absent from the postings drops every covered file, keeps the uncovered one
        Assertions.assertEquals(Arrays.asList("f3.parquet"),
                scanSet(RapCoverage.fromJson(manifest(SNAP, UUID, true, 2000), UUID, SNAP, eq("model", "absent"))));
    }

    // astra CX-29: a posting index outside `files` is a malformed manifest -> nothing may be eliminated
    @Test
    public void testMalformedPostingsKeepEverything() {
        String badIndex = manifest(SNAP, UUID, false, 2000).replaceFirst("\\}$", ", \"postings\": {\"v\": [2]}}");
        RapCoverage c = RapCoverage.fromJson(badIndex, UUID, SNAP, eq("model", "v"));
        Assertions.assertFalse(c.isActive(), "an out-of-range posting index must disable coverage");
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c));
        Assertions.assertEquals(0, c.getDropped());
        String negIndex = manifest(SNAP, UUID, false, 2000).replaceFirst("\\}$", ", \"postings\": {\"v\": [-1]}}");
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"),
                scanSet(RapCoverage.fromJson(negIndex, UUID, SNAP, eq("model", "v"))));
        String emptyList = manifest(SNAP, UUID, false, 2000).replaceFirst("\\}$", ", \"postings\": {\"v\": []}}");
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"),
                scanSet(RapCoverage.fromJson(emptyList, UUID, SNAP, eq("model", "v"))));
        String notArray = manifest(SNAP, UUID, false, 2000).replaceFirst("\\}$", ", \"postings\": {\"v\": 0}}");
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"),
                scanSet(RapCoverage.fromJson(notArray, UUID, SNAP, eq("model", "v"))));
        // a bad posting on an UNRELATED value still disables the whole manifest: a valid-looking elimination
        // must not ride on a manifest that is malformed elsewhere
        String badElsewhere = manifest(SNAP, UUID, false, 2000).replaceFirst("\\}$", ", \"postings\": {\"v\": [0], \"z\": [7]}}");
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"),
                scanSet(RapCoverage.fromJson(badElsewhere, UUID, SNAP, eq("model", "v"))));
        // astra CX-29, second round: numbers that Gson would NARROW to a valid index (0.5, -0.5 and 2^32 all
        // become 0 through getAsInt()) are malformed and must disable coverage -- on the queried value and on
        // an unrelated one alike. With postings {"w": [1]} present, a posting of 0.5 for "v" would otherwise
        // select file 0 and drop f2, the file that actually holds w.
        for (String bad : new String[] {"0.5", "-0.5", "4294967296", "1.0000000001", "-0.0000001", "1e10"}) {
            String queried = manifest(SNAP, UUID, false, 2000)
                    .replaceFirst("\\}$", ", \"postings\": {\"v\": [" + bad + "], \"w\": [1]}}");
            RapCoverage cq = RapCoverage.fromJson(queried, UUID, SNAP, eq("model", "v"));
            Assertions.assertFalse(cq.isActive(), "narrowable posting " + bad + " on the queried value must disable coverage");
            Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(cq), bad);
            String unrelated = manifest(SNAP, UUID, false, 2000)
                    .replaceFirst("\\}$", ", \"postings\": {\"v\": [0], \"z\": [" + bad + "]}}");
            RapCoverage cu = RapCoverage.fromJson(unrelated, UUID, SNAP, eq("model", "v"));
            Assertions.assertFalse(cu.isActive(), "narrowable posting " + bad + " on an unrelated value must disable coverage");
            Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(cu), bad);
        }
        // and exact integers written with a decimal point or exponent are still integers: 1.0 and 1e0 index file 1
        for (String okInt : new String[] {"1.0", "1e0", "1.00"}) {
            String fine = manifest(SNAP, UUID, false, 2000).replaceFirst("\\}$", ", \"postings\": {\"v\": [" + okInt + "]}}");
            RapCoverage cf = RapCoverage.fromJson(fine, UUID, SNAP, eq("model", "v"));
            Assertions.assertTrue(cf.isActive(), okInt);
            Assertions.assertEquals(Arrays.asList("f2.parquet", "f3.parquet"), scanSet(cf), okInt);
        }
    }

    // astra CX-29: the table identity is required, not optional
    @Test
    public void testMissingTableUuidKeepsEverything() {
        String noUuid = manifest(SNAP, UUID, true, 2000).replace("\"table_uuid\": \"" + UUID + "\", ", "");
        RapCoverage c = RapCoverage.fromJson(noUuid, UUID, SNAP, eq("model", "v"));
        Assertions.assertFalse(c.isActive(), "a manifest without table_uuid must not activate");
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(c));
        String emptyUuid = manifest(SNAP, "", true, 2000);
        Assertions.assertFalse(RapCoverage.fromJson(emptyUuid, UUID, SNAP, eq("model", "v")).isActive());
        // a manifest for ANOTHER table never activates (the table-binding leg; it lived in the snapshot case
        // before pass 4 showed mutant V failing both, so each case now guards one protection)
        RapCoverage other = RapCoverage.fromJson(manifest(SNAP, "other-uuid", true, 2000), UUID, SNAP, eq("model", "v"));
        Assertions.assertFalse(other.isActive());
        Assertions.assertEquals(Arrays.asList("f1.parquet", "f2.parquet", "f3.parquet"), scanSet(other));
        // and the expected identity itself must be known: an unknown table never activates
        Assertions.assertFalse(RapCoverage.fromJson(manifest(SNAP, UUID, true, 2000), null, SNAP, eq("model", "v")).isActive());
    }

    private static String tableManifest(String uuid, long snap, List<DataFile> covered, String postings) {
        StringBuilder files = new StringBuilder();
        for (DataFile f : covered) {
            String loc = f.location();
            if (files.length() > 0) {
                files.append(", ");
            }
            files.append("{\"name\": \"").append(loc.substring(loc.lastIndexOf('/') + 1)).append("\", \"size\": ")
                    .append(f.fileSizeInBytes()).append(", \"rows\": ").append(f.recordCount()).append("}");
        }
        return "{\"version\": 1, \"table_uuid\": \"" + uuid + "\", \"snapshot_id\": " + snap + ", \"column\": \"data\", "
                + "\"field_id\": 2, \"granularity_rows\": 20000, \"files\": [" + files + "]"
                + (postings == null ? "" : ", \"postings\": " + postings) + "}";
    }

    // astra CX-32: the REAL loader through the table's own FileIO, and the configuration-off path
    @Test
    public void testLoadThroughTableFileIoAndConfigOff() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).appendFile(FILE_A_2).commit();
        long snap = mockedNativeTableA.currentSnapshot().snapshotId();
        String uuid = ((BaseTable) mockedNativeTableA).operations().current().uuid();
        File dir = Files.createTempDirectory("rap_manifest_").toFile();
        File tdir = new File(dir, uuid);
        Assertions.assertTrue(tdir.mkdirs());
        // covered: data-a, data-a1; postings: v -> data-a only; data-a2 uncovered
        Files.write(new File(tdir, snap + RapCoverage.SUFFIX).toPath(),
                tableManifest(uuid, snap, Arrays.asList(FILE_A, FILE_A_1), "{\"v\": [0]}").getBytes(StandardCharsets.UTF_8));
        CountingFileIO io = new CountingFileIO(mockedNativeTableA.io());
        // config on: the manifest is read through the table's FileIO (here wrapped to count reads) and activates
        RapCoverage on = RapCoverage.load(dir.getAbsolutePath(), io, uuid, Optional.of(snap), eq("data", "v"));
        Assertions.assertTrue(on.isActive(), "the manifest under the configured dir must load through the FileIO");
        Assertions.assertEquals(1, io.opened, "exactly one manifest read");
        Assertions.assertEquals(RapCoverage.Decision.KEEP, on.decide(FILE_A));
        Assertions.assertEquals(RapCoverage.Decision.DROP, on.decide(FILE_A_1));
        Assertions.assertEquals(RapCoverage.Decision.KEEP, on.decide(FILE_A_2));
        // config off (empty dir): the same valid manifest is on disk; NOTHING is read and every file is kept
        io.opened = 0;
        RapCoverage off = RapCoverage.load("", io, uuid, Optional.of(snap), eq("data", "v"));
        Assertions.assertFalse(off.isActive(), "configuration off must not consult the manifest");
        Assertions.assertEquals(0, io.opened, "configuration off must perform no manifest read");
        Assertions.assertEquals(RapCoverage.Decision.KEEP, off.decide(FILE_A_1));
        // config on, another snapshot: no manifest for it -> off (one existence probe, no activation)
        Assertions.assertFalse(
                RapCoverage.load(dir.getAbsolutePath(), io, uuid, Optional.of(snap + 1), eq("data", "v")).isActive());
        // config on, no snapshot known, or no table identity: off without a read
        io.opened = 0;
        Assertions.assertFalse(RapCoverage.load(dir.getAbsolutePath(), io, uuid, Optional.empty(), eq("data", "v")).isActive());
        Assertions.assertFalse(RapCoverage.load(dir.getAbsolutePath(), io, null, Optional.of(snap), eq("data", "v")).isActive());
        Assertions.assertEquals(0, io.opened);
        // the Table-based entry point used by the scan node goes through Config.rap_manifest_dir
        String saved = Config.rap_manifest_dir;
        try {
            Config.rap_manifest_dir = dir.getAbsolutePath();
            Assertions.assertTrue(RapCoverage.load(mockedNativeTableA, Optional.of(snap), eq("data", "v")).isActive());
            Config.rap_manifest_dir = "";
            Assertions.assertFalse(RapCoverage.load(mockedNativeTableA, Optional.of(snap), eq("data", "v")).isActive());
        } finally {
            Config.rap_manifest_dir = saved;
        }
    }

    /** Delegating FileIO that counts opened input files, so "configuration off reads nothing" is observable. */
    private static final class CountingFileIO implements org.apache.iceberg.io.FileIO {
        private final org.apache.iceberg.io.FileIO delegate;
        int opened = 0;

        CountingFileIO(org.apache.iceberg.io.FileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        public org.apache.iceberg.io.InputFile newInputFile(String path) {
            opened++;
            return delegate.newInputFile(path);
        }

        @Override
        public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
        }
    }

    // astra CX-32: the scan-range source actually skips DROP files and keeps the rest
    @Test
    public void testScanRangeSourceSkipsDroppedFiles() throws Exception {
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).appendFile(FILE_A_2).commit();
        long snap = mockedNativeTableA.currentSnapshot().snapshotId();
        String uuid = ((BaseTable) mockedNativeTableA).operations().current().uuid();
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("data", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog", "resource", "db", "table", "",
                schema, mockedNativeTableA, Maps.newHashMap());
        TupleDescriptor td = new TupleDescriptor(new TupleId(7));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), td);
        idSlot.setType(INT);
        idSlot.setColumn(new Column("id", INT));
        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), td);
        dataSlot.setType(VARCHAR);
        dataSlot.setColumn(new Column("data", VARCHAR));
        td.addSlot(idSlot);
        td.addSlot(dataSlot);
        List<FileScanTask> tasks = new ArrayList<>();
        mockedNativeTableA.newScan().planFiles().forEach(tasks::add);
        Assertions.assertEquals(3, tasks.size());

        RapCoverage cov = RapCoverage.fromJson(tableManifest(uuid, snap, Arrays.asList(FILE_A, FILE_A_1), "{\"v\": [0]}"),
                uuid, snap, eq("data", "v"));
        Assertions.assertTrue(cov.isActive());

        List<TScanRangeLocations> with = outputs(icebergTable, td, tasks, cov);
        List<TScanRangeLocations> without = outputs(icebergTable, td, tasks, null);
        Assertions.assertEquals(3, without.size(), "without coverage every planned file is scheduled");
        Assertions.assertEquals(2, with.size(),
                "with coverage the covered no-posting file is not scheduled; the uncovered one is");
        Assertions.assertEquals(1, cov.getDropped());
        Assertions.assertEquals(3, cov.getConsulted());
        // no EQ/IN on the manifest column: coverage active but nothing eliminated
        RapCoverage noPred = RapCoverage.fromJson(tableManifest(uuid, snap, Arrays.asList(FILE_A, FILE_A_1), "{\"v\": [0]}"),
                uuid, snap, eq("id", "v"));
        Assertions.assertEquals(3, outputs(icebergTable, td, tasks, noPred).size());
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

    @Test
    public void testKeyOfIsThePathUnderData() { // slice 2g a-d
        Assertions.assertEquals("x.parquet", RapCoverage.keyOf("gs://b/t/data/x.parquet"));
        Assertions.assertEquals("day=1/bucket=2/x.parquet", RapCoverage.keyOf("gs://b/t/data/day=1/bucket=2/x.parquet"));
        Assertions.assertEquals("p=data/x.parquet", RapCoverage.keyOf("gs://b/data/t/data/p=data/x.parquet"));
        Assertions.assertEquals("x.parquet", RapCoverage.keyOf("gs://b/t/other/x.parquet"));
    }

    @Test
    public void testDecideMatchesPlannedFileByRelativeKey() { // slice 2g h
        DataFile p0 = DataFiles.builder(PartitionSpec.unpartitioned()).withPath("gs://bucket/warehouse/db/t/data/b=0/x.parquet")
                .withFileSizeInBytes(1000).withRecordCount(100).withFormat("PARQUET").build();
        DataFile p1 = DataFiles.builder(PartitionSpec.unpartitioned()).withPath("gs://bucket/warehouse/db/t/data/b=1/x.parquet")
                .withFileSizeInBytes(2000).withRecordCount(200).withFormat("PARQUET").build();
        String m = "{\"version\": 1, \"table_uuid\": \"" + UUID + "\", \"snapshot_id\": " + SNAP + ", \"column\": \"model\", "
                + "\"field_id\": 15, \"granularity_rows\": 20000, \"files\": [{\"name\": \"b=0/x.parquet\", \"size\": 1000, \"rows\": 100}, "
                + "{\"name\": \"b=1/x.parquet\", \"size\": 2000, \"rows\": 200}], \"postings\": {\"v\": [0], \"w\": [1]}}";
        RapCoverage c = RapCoverage.fromJson(m, UUID, SNAP, eq("model", "v"));
        Assertions.assertTrue(c.isActive());
        Assertions.assertEquals(RapCoverage.Decision.KEEP, c.decide(p0));
        Assertions.assertEquals(RapCoverage.Decision.DROP, c.decide(p1));
        Assertions.assertEquals(0, c.getIdentityMismatch());
        Assertions.assertEquals(2, c.getCovered());
    }

    @Test
    public void testExtractLiterals() {
        Assertions.assertEquals(Arrays.asList("v"), RapCoverage.extractLiterals(eq("model", "v"), "model"));
        Assertions.assertEquals(Arrays.asList("v", "w"), RapCoverage.extractLiterals(in(false, "model", "v", "w"), "model"));
        Assertions.assertNull(RapCoverage.extractLiterals(in(true, "model", "v"), "model"));
        Assertions.assertNull(RapCoverage.extractLiterals(eq("brand", "v"), "model"));
        Assertions.assertNull(RapCoverage.extractLiterals(null, "model"));
    }
}
