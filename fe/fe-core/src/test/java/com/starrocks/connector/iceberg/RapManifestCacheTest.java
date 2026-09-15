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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.type.VarcharType.VARCHAR;

/**
 * RAP / lake-index slice 5: the parsed manifest is kept between plans, and a same-snapshot republish is still
 * seen by the next plan.
 *
 * <p>Every case here asserts what the loader DID -- how many times it opened the object, how many times it read
 * the body, and which files the resulting coverage keeps -- not only the outcome label. An outcome label that
 * agrees with a coverage that did not move would be a cache reporting itself correct while serving the wrong
 * manifest, which is the failure this whole class exists to prevent.
 */
public class RapManifestCacheTest {
    private static final String UUID = "11111111-2222-3333-4444-555555555555";
    private static final long SNAP = 6366882456050597382L;
    private static final String DIR = "memory://rap";
    private static final String PATH = DIR + "/" + UUID + "/" + SNAP + RapCoverage.SUFFIX;
    private static final String KEY1 = "gs/bucket/warehouse/db/t/data/f1.parquet";
    private static final String KEY2 = "gs/bucket/warehouse/db/t/data/f2.parquet";

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

    /** C5's PARTIAL: only f1 is covered, so f2 is outside C and is always scanned. */
    private static String partial() {
        return head() + "\"files\": [{\"name\": \"" + KEY1 + "\", \"size\": 1000, \"rows\": 100}], "
                + "\"postings\": {\"v\": [0]}}";
    }

    /** C5's COMPLETE: the SAME snapshot, both files covered, and only f1 posts 'v' -- so f2 becomes droppable. */
    private static String complete() {
        return head() + twoFiles() + "\"postings\": {\"v\": [0]}}";
    }

    /** Byte-for-byte the same LENGTH as {@link #complete()} and a different meaning: 'v' posts to f2, not f1. */
    private static String completeSwapped() {
        return head() + twoFiles() + "\"postings\": {\"v\": [1]}}";
    }

    private static String head() {
        return "{\"version\": 1, \"table_uuid\": \"" + UUID + "\", \"snapshot_id\": " + SNAP + ", "
                + "\"column\": \"model\", ";
    }

    private static String twoFiles() {
        return "\"files\": [{\"name\": \"" + KEY1 + "\", \"size\": 1000, \"rows\": 100}, "
                + "{\"name\": \"" + KEY2 + "\", \"size\": 2000, \"rows\": 200}], ";
    }

    private static ScalarOperator eq(String literal) {
        return new BinaryPredicateOperator(BinaryType.EQ, new ColumnRefOperator(1, VARCHAR, "model", true),
                ConstantOperator.createVarchar(literal));
    }

    private int savedCapacity;
    private long savedTtl;
    private long savedMaxBytes;
    private long savedGeneration;
    private long savedManifestMaxBytes;

    @BeforeEach
    public void setUp() {
        savedCapacity = Config.rap_manifest_cache_capacity;
        savedTtl = Config.rap_manifest_cache_ttl_ms;
        savedMaxBytes = Config.rap_manifest_cache_max_bytes;
        savedGeneration = Config.rap_manifest_generation;
        savedManifestMaxBytes = Config.rap_manifest_max_bytes;
        RapManifestCache.invalidateAll();
        RapManifestCache.resetCounters();
    }

    @AfterEach
    public void tearDown() {
        Config.rap_manifest_cache_capacity = savedCapacity;
        Config.rap_manifest_cache_ttl_ms = savedTtl;
        Config.rap_manifest_cache_max_bytes = savedMaxBytes;
        Config.rap_manifest_generation = savedGeneration;
        Config.rap_manifest_max_bytes = savedManifestMaxBytes;
        RapManifestCache.invalidateAll();
        RapManifestCache.resetCounters();
    }

    private static RapCoverage load(FileIO io) {
        return RapCoverage.load(DIR, io, UUID, Optional.of(SNAP), eq("v"));
    }

    // ------------------------------------------------------------------ hit, miss, and what a hit does not do

    @Test
    public void testSecondPlanIsAHitThatReadsNoBodyAndStillProbesTheObject() {
        CountingFileIO io = new CountingFileIO().put(PATH, complete());

        RapCoverage first = load(io);
        Assertions.assertEquals("miss", first.getCache());
        Assertions.assertEquals(RapCoverage.Decision.KEEP, first.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.DROP, first.decide(F2));
        Assertions.assertEquals(1, io.opened);
        Assertions.assertEquals(1, io.bodyReads);
        Assertions.assertEquals(1, RapManifestCache.size());
        Assertions.assertEquals(complete().length(), RapManifestCache.bytes());

        RapCoverage second = load(io);
        Assertions.assertEquals("hit", second.getCache());
        // the coverage is the same one the manifest describes -- a hit that served nothing useful is not a hit
        Assertions.assertEquals(RapCoverage.Decision.KEEP, second.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.DROP, second.decide(F2));
        // the object WAS probed again (default TTL 0), and its body was NOT read again
        Assertions.assertEquals(2, io.opened, "the default TTL re-probes the object on every plan");
        Assertions.assertEquals(2, io.existsCalls);
        Assertions.assertEquals(2, io.lengthCalls, "one length probe per plan, the call the loader already made");
        Assertions.assertEquals(1, io.bodyReads, "a hit must not read the manifest body");
        Assertions.assertEquals(1, RapManifestCache.hits());
        Assertions.assertEquals(1, RapManifestCache.misses());
    }

    @Test
    public void testTheCachedParseIsBoundSeparatelyForEachPlansPredicate() {
        // the manifest is parsed once; each plan's own literals are what select the matching files
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        RapCoverage hitV = RapCoverage.load(DIR, io, UUID, Optional.of(SNAP), eq("v"));
        RapCoverage hitAbsent = RapCoverage.load(DIR, io, UUID, Optional.of(SNAP), eq("absent"));
        Assertions.assertEquals("miss", hitV.getCache());
        Assertions.assertEquals("hit", hitAbsent.getCache());
        Assertions.assertEquals(1, io.bodyReads);
        // 'v' posts to f1 only: f2 drops. 'absent' posts nowhere: BOTH drop.
        Assertions.assertEquals(RapCoverage.Decision.KEEP, hitV.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.DROP, hitV.decide(F2));
        Assertions.assertEquals(RapCoverage.Decision.DROP, hitAbsent.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.DROP, hitAbsent.decide(F2));
    }

    // ------------------------------------------------------------------ C5: same-snapshot republication

    @Test
    public void testSameSnapshotRepublishIsVisibleOnTheVeryNextPlan() {
        // C5's transition: the manifest at ONE path is replaced with a version covering more of the same
        // snapshot's files, and nothing else is touched -- no configuration write, no invalidation call.
        CountingFileIO io = new CountingFileIO().put(PATH, partial());
        RapCoverage before = load(io);
        Assertions.assertEquals("miss", before.getCache());
        Assertions.assertEquals(RapCoverage.Decision.KEEP, before.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.KEEP, before.decide(F2), "f2 is outside C, so it is scanned");

        io.put(PATH, complete());                       // the publication, and NOTHING else

        RapCoverage after = load(io);
        Assertions.assertEquals("refresh", after.getCache(), "the object moved, so the parse must not be reused");
        Assertions.assertEquals(RapCoverage.Decision.KEEP, after.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.DROP, after.decide(F2), "the new coverage must be the one used");
        Assertions.assertEquals(2, io.bodyReads);
        Assertions.assertEquals(1, RapManifestCache.refreshes());
        Assertions.assertEquals(0, RapManifestCache.hits());
        Assertions.assertEquals(complete().length(), RapManifestCache.bytes());
    }

    @Test
    public void testSameLengthRepublishIsSeenWhenTheFileIoExposesAModificationTime() throws Exception {
        // gs:// and file:// through a HadoopFileIO give a HadoopInputFile, whose getStat() carries a modification
        // time from the getFileStatus the length probe already made. That is the strong stamp.
        File dir = Files.createTempDirectory("rap_cache_mtime_").toFile();
        File tdir = new File(dir, UUID);
        Assertions.assertTrue(tdir.mkdirs());
        File manifest = new File(tdir, SNAP + RapCoverage.SUFFIX);
        Files.write(manifest.toPath(), complete().getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(manifest.setLastModified(1_600_000_000_000L));
        HadoopFileIO io = new HadoopFileIO(new Configuration());
        String dirUri = dir.getAbsolutePath();

        RapCoverage before = RapCoverage.load(dirUri, io, UUID, Optional.of(SNAP), eq("v"));
        Assertions.assertEquals("miss", before.getCache());
        Assertions.assertEquals(RapCoverage.Decision.KEEP, before.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.DROP, before.decide(F2));

        // the republish keeps the byte count EXACTLY and changes the meaning: 'v' now posts to f2, not f1
        Files.write(manifest.toPath(), completeSwapped().getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(complete().length(), completeSwapped().length(), "the two must be the same length");
        Assertions.assertTrue(manifest.setLastModified(1_600_000_060_000L));

        RapCoverage after = RapCoverage.load(dirUri, io, UUID, Optional.of(SNAP), eq("v"));
        Assertions.assertEquals("refresh", after.getCache(), "the modification time moved, so the parse is dropped");
        Assertions.assertEquals(RapCoverage.Decision.DROP, after.decide(F1));
        Assertions.assertEquals(RapCoverage.Decision.KEEP, after.decide(F2));
    }

    @Test
    public void testSameLengthRepublishIsMissedWhenTheFileIoExposesOnlyLength() {
        // The DEPLOYED frontend reads through IcebergCachingFileIO, whose CachingInputFile is private and offers
        // exists() and getLength() and nothing else. This pins the consequence so it cannot change silently: an
        // equal-LENGTH same-snapshot republish is NOT seen. Every publication this project has measured changes
        // the length (C5: 94,766 -> 124,863 bytes), and rap_manifest_cache_capacity = 0 is the way out for a
        // publisher that cannot promise that. If this case ever starts reporting "refresh", the stamp got
        // stronger and this expectation -- not the engine -- is what should change.
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        RapCoverage before = load(io);
        Assertions.assertEquals("miss", before.getCache());
        Assertions.assertEquals(RapCoverage.Decision.DROP, before.decide(F2));

        io.put(PATH, completeSwapped());
        Assertions.assertEquals(complete().length(), completeSwapped().length(), "the two must be the same length");

        RapCoverage after = load(io);
        Assertions.assertEquals("hit", after.getCache(), "length-only freshness cannot see an equal-length republish");
        Assertions.assertEquals(RapCoverage.Decision.KEEP, after.decide(F1), "the OLD coverage is what still serves");
        Assertions.assertEquals(RapCoverage.Decision.DROP, after.decide(F2));
        Assertions.assertEquals("len", RapManifestCache.stampOf(io.newInputFile(PATH)).kind());
    }

    // ------------------------------------------------------------------ the TTL, and the generation lever

    @Test
    public void testATtlSkipsTheProbeEntirelyAndExpiryBringsItBack() throws Exception {
        Config.rap_manifest_cache_ttl_ms = 60_000;
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        Assertions.assertEquals("miss", load(io).getCache());
        Assertions.assertEquals(1, io.opened);

        Assertions.assertEquals("hit", load(io).getCache());
        Assertions.assertEquals(1, io.opened, "inside the TTL the object is not touched at all");
        Assertions.assertEquals(1, io.bodyReads);

        Config.rap_manifest_cache_ttl_ms = 1;
        Thread.sleep(10);
        RapCoverage expired = load(io);
        Assertions.assertEquals("hit", expired.getCache(), "the object had not moved, so the parse is still good");
        Assertions.assertEquals(2, io.opened, "past the TTL the object is probed again");
        Assertions.assertEquals(1, io.bodyReads, "and its body is still not re-read");
        Assertions.assertEquals(RapCoverage.Decision.DROP, expired.decide(F2));
    }

    @Test
    public void testInsideATtlARepublishIsServedStaleUntilTheGenerationIsBumped() {
        // This is the whole reason the TTL defaults to zero. Nothing signals this frontend when a manifest is
        // replaced (harness/manifest_to_fe.py is a file copy onto the pod), so a TTL above zero WILL serve the
        // old parse inside its window. The generation bump is the only lever that closes it without a round trip.
        Config.rap_manifest_cache_ttl_ms = 60_000;
        CountingFileIO io = new CountingFileIO().put(PATH, partial());
        RapCoverage before = load(io);
        Assertions.assertEquals("miss", before.getCache());
        Assertions.assertEquals(RapCoverage.Decision.KEEP, before.decide(F2));

        io.put(PATH, complete());
        RapCoverage stale = load(io);
        Assertions.assertEquals("hit", stale.getCache());
        Assertions.assertEquals(RapCoverage.Decision.KEEP, stale.decide(F2), "the republish is NOT visible inside a TTL");
        Assertions.assertEquals(1, io.opened);

        Config.rap_manifest_generation = Config.rap_manifest_generation + 1;
        RapCoverage fresh = load(io);
        Assertions.assertEquals("miss", fresh.getCache(), "a generation bump drops the entry, so this is a fresh load");
        Assertions.assertEquals(RapCoverage.Decision.DROP, fresh.decide(F2), "and the new coverage is what serves");
        Assertions.assertEquals(2, io.opened);
        Assertions.assertEquals(2, io.bodyReads);
    }

    // ------------------------------------------------------------------ what is never cached as valid

    @Test
    public void testAnInvalidManifestIsNeverCachedAndDropsWhatWasHeld() {
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        Assertions.assertEquals("miss", load(io).getCache());
        Assertions.assertTrue(RapManifestCache.holds(PATH));

        // a posting index outside the manifest's own files. Deliberately a DIFFERENT length from the good one:
        // an equal-length replacement is not seen at all through a length-only stamp, which
        // testSameLengthRepublishIsMissedWhenTheFileIoExposesOnlyLength covers on its own terms.
        io.put(PATH, head() + twoFiles() + "\"postings\": {\"v\": [0, 9]}}");
        RapCoverage refused = load(io);
        Assertions.assertFalse(refused.isActive(), "a malformed manifest keeps every file");
        Assertions.assertEquals(RapCoverage.Decision.KEEP, refused.decide(F2));
        Assertions.assertFalse(RapManifestCache.holds(PATH), "and it must not leave the previous parse serving plans");
        Assertions.assertEquals(0, RapManifestCache.size());

        // and the entry really is gone: putting the good manifest back is a MISS, not a hit on a stale entry
        io.put(PATH, complete());
        Assertions.assertEquals("miss", load(io).getCache());
    }

    @Test
    public void testAnOversizeManifestIsNeverCachedAndDropsWhatWasHeld() {
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        Assertions.assertEquals("miss", load(io).getCache());
        Assertions.assertTrue(RapManifestCache.holds(PATH));

        Config.rap_manifest_max_bytes = complete().length() - 1;
        RapCoverage refused = load(io);
        Assertions.assertFalse(refused.isActive(), "above rap_manifest_max_bytes the manifest is off");
        Assertions.assertFalse(RapManifestCache.holds(PATH));
        Assertions.assertEquals(1, io.bodyReads, "an oversize manifest is refused BEFORE the read, not parsed in part");
    }

    @Test
    public void testAManifestThatDisappearsDropsWhatWasHeld() {
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        Assertions.assertEquals("miss", load(io).getCache());
        io.remove(PATH);
        Assertions.assertFalse(load(io).isActive());
        Assertions.assertFalse(RapManifestCache.holds(PATH), "an absent manifest must not serve from the cache");
    }

    @Test
    public void testAnInvalidatedEntryIsNeverServedAgain() {
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        Assertions.assertEquals("miss", load(io).getCache());
        RapManifestCache.invalidate(PATH);
        Assertions.assertFalse(RapManifestCache.holds(PATH));
        Assertions.assertEquals("miss", load(io).getCache());
        Assertions.assertEquals(2, io.bodyReads);
    }

    // ------------------------------------------------------------------ bounds

    @Test
    public void testCapacityEvictsTheLeastRecentlyUsedManifest() {
        Config.rap_manifest_cache_capacity = 2;
        CountingFileIO io = new CountingFileIO();
        String[] paths = new String[3];
        for (int i = 0; i < 3; i++) {
            paths[i] = DIR + "/" + UUID + "/" + (SNAP + i) + RapCoverage.SUFFIX;
            io.put(paths[i], complete().replace("\"snapshot_id\": " + SNAP, "\"snapshot_id\": " + (SNAP + i)));
        }
        RapCoverage.load(DIR, io, UUID, Optional.of(SNAP), eq("v"));
        RapCoverage.load(DIR, io, UUID, Optional.of(SNAP + 1), eq("v"));
        // touch the first one so the SECOND is the least recently used
        Assertions.assertEquals("hit", RapCoverage.load(DIR, io, UUID, Optional.of(SNAP), eq("v")).getCache());
        RapCoverage.load(DIR, io, UUID, Optional.of(SNAP + 2), eq("v"));

        Assertions.assertEquals(2, RapManifestCache.size());
        Assertions.assertTrue(RapManifestCache.holds(paths[0]));
        Assertions.assertFalse(RapManifestCache.holds(paths[1]), "the least recently USED entry is the one evicted");
        Assertions.assertTrue(RapManifestCache.holds(paths[2]));
        // the evicted one comes back as a miss, not as a hit on something stale
        Assertions.assertEquals("miss", RapCoverage.load(DIR, io, UUID, Optional.of(SNAP + 1), eq("v")).getCache());
    }

    @Test
    public void testTheByteBoundEvictsEvenWhenTheCountFits() {
        Config.rap_manifest_cache_capacity = 8;
        Config.rap_manifest_cache_max_bytes = complete().length() + 1;      // room for one manifest, not two
        CountingFileIO io = new CountingFileIO();
        String a = DIR + "/" + UUID + "/" + SNAP + RapCoverage.SUFFIX;
        String b = DIR + "/" + UUID + "/" + (SNAP + 1) + RapCoverage.SUFFIX;
        io.put(a, complete());
        io.put(b, complete().replace("\"snapshot_id\": " + SNAP, "\"snapshot_id\": " + (SNAP + 1)));

        RapCoverage.load(DIR, io, UUID, Optional.of(SNAP), eq("v"));
        Assertions.assertEquals(1, RapManifestCache.size());
        RapCoverage.load(DIR, io, UUID, Optional.of(SNAP + 1), eq("v"));
        Assertions.assertEquals(1, RapManifestCache.size(), "the byte bound binds before the count does");
        Assertions.assertFalse(RapManifestCache.holds(a));
        Assertions.assertTrue(RapManifestCache.holds(b));
        Assertions.assertTrue(RapManifestCache.bytes() <= Config.rap_manifest_cache_max_bytes);
    }

    @Test
    public void testCapacityZeroIsExactlyTheBehaviourBeforeTheCacheExisted() {
        Config.rap_manifest_cache_capacity = 0;
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        RapCoverage first = load(io);
        RapCoverage second = load(io);
        Assertions.assertEquals("disabled", first.getCache());
        Assertions.assertEquals("disabled", second.getCache());
        Assertions.assertEquals(2, io.bodyReads, "with the cache off every plan reads and parses the manifest");
        Assertions.assertEquals(0, RapManifestCache.size());
        Assertions.assertEquals(RapCoverage.Decision.DROP, second.decide(F2));
    }

    @Test
    public void testTurningTheCapacityDownReleasesWhatIsHeld() {
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        load(io);
        Assertions.assertEquals(1, RapManifestCache.size());
        Assertions.assertTrue(RapManifestCache.bytes() > 0);
        Config.rap_manifest_cache_capacity = 0;
        load(io);
        Assertions.assertEquals(0, RapManifestCache.size());
        Assertions.assertEquals(0, RapManifestCache.bytes());
    }

    // ------------------------------------------------------------------ the evidence a run can read

    @Test
    public void testThePlanLineCarriesTheOutcomeAndKeepsEveryFieldThatWasThereBefore() {
        CountingFileIO io = new CountingFileIO().put(PATH, complete());
        String miss = load(io).explain();
        String hit = load(io).explain();
        Assertions.assertEquals("RAP MANIFEST: active snapshot=" + SNAP + " column=model postings=yes consulted=0 "
                + "covered=0 identity_mismatch=0 dropped=0 reason=ok hints=no cache=miss", miss);
        Assertions.assertTrue(hit.endsWith(" cache=hit"), hit);
        // the fields the retained parsers read are unchanged and still in front of the new one
        Assertions.assertTrue(hit.startsWith("RAP MANIFEST: active snapshot=" + SNAP + " column=model postings=yes "), hit);
        Assertions.assertTrue(RapCoverage.disabled(SNAP).explain().endsWith(" cache=none"));
    }

    // ------------------------------------------------------------------ a FileIO that says what was asked of it

    /**
     * A FileIO over an in-memory map that counts what the loader did: how many input files it opened, how many
     * existence and length probes it made, and how many times it read a body. Its InputFile exposes only what
     * {@code InputFile} declares, which is what the deployed {@code IcebergCachingFileIO.CachingInputFile} does.
     */
    private static final class CountingFileIO implements FileIO {
        private final Map<String, byte[]> contents = new HashMap<>();
        private int opened = 0;
        private int existsCalls = 0;
        private int lengthCalls = 0;
        private int bodyReads = 0;

        CountingFileIO put(String path, String content) {
            contents.put(path, content.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        void remove(String path) {
            contents.remove(path);
        }

        @Override
        public InputFile newInputFile(String path) {
            opened++;
            return new CountingInputFile(path, contents.get(path));
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            contents.remove(path);
        }

        private final class CountingInputFile implements InputFile {
            private final String path;
            private final byte[] content;

            private CountingInputFile(String path, byte[] content) {
                this.path = path;
                this.content = content;
            }

            @Override
            public long getLength() {
                lengthCalls++;
                if (content == null) {
                    throw new IllegalStateException("no such manifest: " + path);
                }
                return content.length;
            }

            @Override
            public SeekableInputStream newStream() {
                bodyReads++;
                return new InMemoryInputFile(path, content).newStream();
            }

            @Override
            public String location() {
                return path;
            }

            @Override
            public boolean exists() {
                existsCalls++;
                return content != null;
            }
        }
    }
}
