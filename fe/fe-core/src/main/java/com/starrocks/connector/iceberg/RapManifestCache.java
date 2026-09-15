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
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RAP / lake-index slice 5: the parsed RAP manifest, kept between plans.
 *
 * <p><b>Why.</b> {@code RapCoverage.load} read and parsed the whole manifest on every plan, and the Iceberg
 * metadata content cache that would otherwise hold the bytes is off by upstream default
 * ({@code iceberg_metadata_memory_cache_capacity = 0}, {@code iceberg_metadata_cache_max_entry_size = 0}).
 * Measured on {@code pg_many} at 256 files: +47.6 ms of planning per plan for a 2,479,240-byte manifest,
 * ~15-20 ms per manifest megabyte. What is paid per plan is a JSON tokenisation of the whole object plus an
 * exact-integer validation of every posting index -- 310,010 of them at 256 files, 2,041,631 at 1,500 -- and
 * none of that depends on the query. So it is done once and kept; only the predicate binding is per plan.
 *
 * <p><b>The freshness rule, and why it is not a TTL by default.</b> A RAP manifest is published by replacing
 * the object AT ITS OWN PATH for the same snapshot ({@code harness/manifest_to_fe.py}: a file copy onto the FE
 * pod, an object write elsewhere). There is <b>no signal to the FE</b> -- no notification, no lease, no
 * catalog commit -- so nothing can invalidate this cache on publication. C5 requires the next plan after such
 * a republish to see the new coverage. Therefore the default is {@code rap_manifest_cache_ttl_ms = 0}:
 * <b>every plan re-probes the object</b> and reuses the parse only while the probe's stamp is unchanged. That
 * probe is {@code newInputFile} + {@code exists()} + {@code getLength()} -- exactly the calls the loader
 * already made before this cache existed -- so the steady state costs no MORE round trips than before, and
 * saves the body read and the parse.
 *
 * <p>A TTL above zero skips even that probe and is the only setting that costs no remote call at all. It is
 * safe only when no same-snapshot in-place republish can land inside the window, or when the publisher bumps
 * {@code rap_manifest_generation} after publishing -- which invalidates every entry here with no round trip,
 * the same lever the backend's sidecar cache uses.
 *
 * <p><b>The stamp is the strongest thing {@link InputFile} exposes, which is not much.</b> Iceberg's
 * {@code InputFile} has no generation, ETag or modification time; it has {@code exists()} and
 * {@code getLength()}. When the file is a {@link HadoopInputFile} its {@code getStat()} adds a modification
 * time from the same {@code getFileStatus} call, and the stamp uses it. On the deployed FE the table's
 * {@code FileIO} is {@code IcebergCachingFileIO}, which hands out a {@code CachingInputFile}; that wrapper now
 * exposes the file it caches ({@code CachingInputFile.wrapped()}), so the deployed stamp is <b>len+mtime</b>
 * too, and an equal-length same-snapshot republish is seen. <b>Length alone is the fallback</b>, for a
 * {@code FileIO} that genuinely exposes no modification time: there a republish that changes the content
 * without changing the byte count is missed, and {@code rap_manifest_cache_capacity = 0} is the way out for a
 * publisher that cannot promise the length moves. The stamp kind is reported in the plan
 * ({@code cache=hit}/{@code refresh}) and in the log, so a run can see which was used rather than assume.
 *
 * <p><b>Bounds.</b> At most {@code rap_manifest_cache_capacity} manifests and
 * {@code rap_manifest_cache_max_bytes} of manifest source bytes, whichever binds first; least-recently-used
 * out. Capacity zero turns the cache off and drops what it holds. A manifest that is absent, above
 * {@code rap_manifest_max_bytes}, or that fails to parse is never stored, and invalidates any entry for the
 * same path -- a refused manifest must not leave the previous one serving plans.
 */
public final class RapManifestCache {
    private static final Logger LOG = LogManager.getLogger(RapManifestCache.class);

    /** No modification time is available from this {@link InputFile}; the stamp is length only. */
    public static final long MTIME_UNKNOWN = Long.MIN_VALUE;

    /** What served a plan's manifest. Rendered into the plan's {@code RAP MANIFEST:} line as {@code cache=...}. */
    public enum Outcome {
        /** No manifest was consulted at all: the feature is off, or there is no table identity or snapshot. */
        NONE("none"),
        /** {@code rap_manifest_cache_capacity <= 0}: the manifest was read and parsed, and nothing was kept. */
        DISABLED("disabled"),
        /** Nothing was cached for this manifest: it was read and parsed, and stored. */
        MISS("miss"),
        /** Served from the cache: no body read and no parse. */
        HIT("hit"),
        /** An entry existed and was stale (the object moved, or the generation was bumped): re-read and re-parsed. */
        REFRESH("refresh");

        private final String label;

        Outcome(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    /** What a probe of the manifest object established about it. */
    public static final class Stamp {
        private final long length;
        private final long mtimeMillis;

        Stamp(long length, long mtimeMillis) {
            this.length = length;
            this.mtimeMillis = mtimeMillis;
        }

        public long length() {
            return length;
        }

        public long mtimeMillis() {
            return mtimeMillis;
        }

        /** "len+mtime" when a modification time was available, "len" when the FileIO exposes none. */
        public String kind() {
            return mtimeMillis == MTIME_UNKNOWN ? "len" : "len+mtime";
        }

        /**
         * Exact equality on every component. A stamp with no modification time never matches one that has a
         * modification time: losing the stronger signal is a reason to re-read, not to reuse.
         */
        public boolean matches(Stamp other) {
            return other != null && length == other.length && mtimeMillis == other.mtimeMillis;
        }

        @Override
        public String toString() {
            return mtimeMillis == MTIME_UNKNOWN ? ("len=" + length) : ("len=" + length + " mtime=" + mtimeMillis);
        }
    }

    /**
     * A snapshot of what the cache held for one path, taken under the lock so the decision the caller makes
     * cannot be invalidated underneath it.
     */
    public static final class Lookup {
        private final RapCoverage.Parsed parsed;
        private final Stamp stamp;
        private final boolean freshWithoutProbe;

        private Lookup(RapCoverage.Parsed parsed, Stamp stamp, boolean freshWithoutProbe) {
            this.parsed = parsed;
            this.stamp = stamp;
            this.freshWithoutProbe = freshWithoutProbe;
        }

        /** True when an entry for this path is held at the current generation. */
        public boolean present() {
            return parsed != null;
        }

        /** True when the entry may be reused with NO probe of the object: inside {@code rap_manifest_cache_ttl_ms}. */
        public boolean freshWithoutProbe() {
            return freshWithoutProbe;
        }

        public RapCoverage.Parsed parsed() {
            return parsed;
        }

        /** True when the object's stamp is the one this entry was parsed from, so the parse may be reused. */
        public boolean matches(Stamp probed) {
            return parsed != null && stamp != null && stamp.matches(probed);
        }

        public Stamp stamp() {
            return stamp;
        }
    }

    private static final Lookup ABSENT = new Lookup(null, null, false);

    private static final class Entry {
        private final RapCoverage.Parsed parsed;
        private final Stamp stamp;
        private final long generation;
        private final long bytes;
        private long validatedAtNanos;

        private Entry(RapCoverage.Parsed parsed, Stamp stamp, long generation, long bytes, long validatedAtNanos) {
            this.parsed = parsed;
            this.stamp = stamp;
            this.generation = generation;
            this.bytes = bytes;
            this.validatedAtNanos = validatedAtNanos;
        }
    }

    // access-ordered, so the eldest entry is the least recently USED one, not the least recently inserted
    private static final LinkedHashMap<String, Entry> ENTRIES = new LinkedHashMap<>(16, 0.75f, true);
    private static final Object LOCK = new Object();
    private static long cachedBytes = 0;
    private static long hits = 0;
    private static long misses = 0;
    private static long refreshes = 0;

    private RapManifestCache() {
    }

    /** True while the cache is configured on. Capacity zero or below is "off" and holds nothing. */
    public static boolean enabled() {
        return Config.rap_manifest_cache_capacity > 0;
    }

    /**
     * Probe the object for the freshness signal, using the strongest one this {@link InputFile} exposes.
     * {@code getLength()} is the call the loader already made; {@code HadoopInputFile.getStat()} reuses the
     * {@code FileStatus} that call fetched, so the modification time costs nothing extra where it exists.
     *
     * <p>The deployed FE reads through {@code IcebergCachingFileIO}, so the file handed here is a
     * {@code CachingInputFile} wrapping the real one; it is unwrapped first, which is what makes the deployed
     * stamp {@code len+mtime} rather than length alone. A {@code FileIO} that exposes no modification time at
     * all still gets the length-only stamp.
     */
    public static Stamp stampOf(InputFile in) {
        long length = in.getLength();
        long mtime = MTIME_UNKNOWN;
        InputFile stated = in instanceof IcebergCachingFileIO.CachingInputFile
                ? ((IcebergCachingFileIO.CachingInputFile) in).wrapped() : in;
        if (stated instanceof HadoopInputFile) {
            try {
                mtime = ((HadoopInputFile) stated).getStat().getModificationTime();
                if (mtime == MTIME_UNKNOWN) {
                    mtime = MTIME_UNKNOWN + 1;   // keep the sentinel meaning "no modification time available"
                }
            } catch (Exception e) {
                mtime = MTIME_UNKNOWN;
            }
        }
        return new Stamp(length, mtime);
    }

    /**
     * What is held for {@code path} right now. Never null. When the cache is off this also drops everything it
     * holds, so turning the capacity down releases the memory rather than stranding it.
     */
    public static Lookup lookup(String path) {
        if (!enabled()) {
            invalidateAll();
            return ABSENT;
        }
        long generation = Config.rap_manifest_generation;
        long ttlMs = Config.rap_manifest_cache_ttl_ms;
        synchronized (LOCK) {
            Entry e = ENTRIES.get(path);
            if (e == null) {
                return ABSENT;
            }
            if (e.generation != generation) {
                // a generation bump invalidates every entry with no round trip -- the FE mirror of the BE's
                // rap_index_generation lever
                remove(path);
                return ABSENT;
            }
            boolean fresh = ttlMs > 0 && (System.nanoTime() - e.validatedAtNanos) < ttlMs * 1_000_000L;
            return new Lookup(e.parsed, e.stamp, fresh);
        }
    }

    /** Record that the object was probed and had not moved, so the entry stays warm for the TTL. */
    public static void confirm(String path) {
        synchronized (LOCK) {
            Entry e = ENTRIES.get(path);
            if (e != null) {
                e.validatedAtNanos = System.nanoTime();
            }
        }
    }

    /** Count one plan's outcome. Called exactly once per plan that consulted a manifest. */
    public static void count(Outcome outcome) {
        synchronized (LOCK) {
            switch (outcome) {
                case HIT:
                    hits++;
                    break;
                case REFRESH:
                    refreshes++;
                    break;
                case MISS:
                case DISABLED:
                    misses++;
                    break;
                default:
                    break;
            }
        }
    }

    /** Store a freshly parsed manifest under its stamp, then bring the cache back inside its bounds. */
    public static void store(String path, RapCoverage.Parsed parsed, Stamp stamp, long bytes) {
        if (!enabled()) {
            return;
        }
        synchronized (LOCK) {
            remove(path);
            ENTRIES.put(path, new Entry(parsed, stamp, Config.rap_manifest_generation, bytes, System.nanoTime()));
            cachedBytes += bytes;
            evictToBounds();
        }
    }

    /** Drop whatever is held for one path. A refused manifest must not leave the previous one serving plans. */
    public static void invalidate(String path) {
        synchronized (LOCK) {
            remove(path);
        }
    }

    public static void invalidateAll() {
        synchronized (LOCK) {
            ENTRIES.clear();
            cachedBytes = 0;
        }
    }

    /** Entries held. Test and diagnostic surface. */
    public static int size() {
        synchronized (LOCK) {
            return ENTRIES.size();
        }
    }

    /** Manifest source bytes held. Test and diagnostic surface. */
    public static long bytes() {
        synchronized (LOCK) {
            return cachedBytes;
        }
    }

    public static boolean holds(String path) {
        synchronized (LOCK) {
            return ENTRIES.containsKey(path);
        }
    }

    public static long hits() {
        synchronized (LOCK) {
            return hits;
        }
    }

    public static long misses() {
        synchronized (LOCK) {
            return misses;
        }
    }

    public static long refreshes() {
        synchronized (LOCK) {
            return refreshes;
        }
    }

    public static void resetCounters() {
        synchronized (LOCK) {
            hits = 0;
            misses = 0;
            refreshes = 0;
        }
    }

    /**
     * One line per plan that consulted a manifest, at INFO, carrying the table, the snapshot, the outcome and
     * the stamp that decided it. The plan's own {@code RAP MANIFEST:} line carries the outcome too; this
     * carries the stamp, which the plan line has no room for.
     */
    static void log(Outcome outcome, String uuid, long snapshot, Stamp stamp, long bytes) {
        LOG.info("RAP manifest cache {} for {} snapshot {} ({} {}, {} bytes; entries {}/{}, {} bytes)",
                outcome.label(), uuid, snapshot, stamp == null ? "no probe" : stamp.kind(),
                stamp == null ? "-" : stamp.toString(), bytes, size(), Config.rap_manifest_cache_capacity, bytes());
    }

    private static void remove(String path) {
        Entry old = ENTRIES.remove(path);
        if (old != null) {
            cachedBytes -= old.bytes;
        }
    }

    private static void evictToBounds() {
        int capacity = Config.rap_manifest_cache_capacity;
        long maxBytes = Config.rap_manifest_cache_max_bytes;
        Iterator<Map.Entry<String, Entry>> it = ENTRIES.entrySet().iterator();
        // never evict the entry just stored: one manifest larger than the byte bound is kept for this plan's
        // successors rather than thrashing, and the single-manifest ceiling is rap_manifest_max_bytes
        while (ENTRIES.size() > Math.max(capacity, 1) || (ENTRIES.size() > 1 && cachedBytes > maxBytes)) {
            if (!it.hasNext()) {
                return;
            }
            Map.Entry<String, Entry> eldest = it.next();
            cachedBytes -= eldest.getValue().bytes;
            it.remove();
        }
    }
}
