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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * RAP / lake-index slice 2b: the per-snapshot manifest and the completeness rule.
 *
 * <p>For a scan at snapshot S over planned files P, the manifest names the files C that carry a
 * registered sidecar index (with each file's identity: storage-qualified key, size, rows) and, optionally, the
 * postings value -> files for the indexed column. With EQ / IN literals on that column, M is the union
 * of the literals' files and the scan set is M ∪ (P − C): a covered file with no posting is dropped,
 * an uncovered file is ALWAYS kept. Every doubt keeps the file: no manifest for S, unreadable or
 * unknown-version manifest, identity mismatch, no postings, no EQ / IN on the column, NOT IN.
 *
 * <p>Deletes: position and equality deletes remove rows after the read; the index only selects which
 * rows are read; a file with no matching row stays droppable under any delete set.
 *
 * <p>Manifest v2 uses lowercase hexadecimal RAPX canonical keys with an explicit key type and NULL
 * file postings. It also supports ordered comparisons, NULL tests and conjunctions on its indexed
 * column. V1 remains string EQ/IN only. Unsupported shapes keep the ordinary scan.
 *
 * <p>Disabled unless {@code Config.rap_manifest_dir} is set. A disabled or failed coverage keeps
 * every file and counts nothing but {@code consulted}.
 *
 * <p><b>Slice 5 (workstream D):</b> everything a manifest establishes before the query's literals are looked at
 * is done once, in {@link #parseManifest}, and kept between plans by {@link RapManifestCache}; a plan pays only
 * {@link #bind}. That split is what the row-range work of workstream A sits inside: the {@code granules} section
 * is VALIDATED -- and turned into row ranges -- at parse time, with the rest of the document, and only the
 * selection of which ranges this query's literals choose happens per plan.
 */
public class RapCoverage {
    private static final Logger LOG = LogManager.getLogger(RapCoverage.class);
    public static final int VERSION = 1;
    public static final int TYPED_VERSION = 2;
    /**
     * Manifest v3 = v2 plus per-file GRANULE ORDINALS (R7, plan-time prefetch). A v3 manifest carries, next to
     * {@code postings} (value -> file ordinals), a {@code granules} object (value -> file ordinal -> the granule
     * ordinals of that file that hold the value), a {@code null_granules} object for the NULL posting, and a
     * {@code granularity} on every entry of {@code files}.
     *
     * <p>Granule {@code g} of a file of {@code rows} rows is {@code [g * granularity, min((g + 1) * granularity,
     * rows))}, and adjacent granules are coalesced into one range. That reproduces exactly the row ranges the
     * per-file sidecar the manifest was built from would have answered -- every sidecar's postings are
     * granule-aligned -- so a covered file can be narrowed at PLANNING and the backend never opens its sidecar,
     * while one small integer per granule replaces a pair of int64 row positions. On the 1,500-file density
     * fixture that is 45 MB rather than 175 MB, which is the difference between fitting
     * {@code rap_manifest_max_bytes} and not.
     *
     * <p>The derivation happens ONCE, in {@link #parseRanges}, so everything below it -- {@code matchingRanges},
     * {@link #rowRangesFor}, {@link #bind} and the backend -- still sees {@code long[][]} row ranges and is
     * unaffected. v1 and v2 manifests carry no granules and therefore produce no hints: exactly today's
     * behaviour, with the backend consulting the sidecar itself.
     */
    public static final int RANGED_VERSION = 3;
    public static final String SUFFIX = ".rapm.json";

    public enum Decision { KEEP, DROP }

    // Fallback reasons, as single tokens. `disabled` is the ordinary "no rap_manifest_dir configured" state and is
    // not logged; every other value means a manifest was expected and something about it kept the ordinary scan.
    public static final String REASON_OK = "ok";
    public static final String REASON_DISABLED = "disabled";
    public static final String REASON_NO_SNAPSHOT = "no_snapshot";
    public static final String REASON_NO_TABLE = "no_table";
    public static final String REASON_NO_TABLE_UUID = "no_table_uuid";
    public static final String REASON_OTHER_TABLE = "other_table";
    public static final String REASON_OTHER_SNAPSHOT = "other_snapshot";
    public static final String REASON_ABSENT = "absent";
    public static final String REASON_OVERSIZE = "oversize";
    public static final String REASON_UNREADABLE = "unreadable";
    public static final String REASON_UNSUPPORTED_VERSION = "unsupported_version";
    public static final String REASON_MALFORMED = "malformed";
    public static final String REASON_STALE_COLUMN = "stale_column_identity";
    /** Active, but the query constrains no column this manifest indexes: nothing is eliminated and nothing is hinted. */
    public static final String REASON_PREDICATE_NOT_BOUND = "predicate_not_on_indexed_column";
    /** Active, but the manifest carries no postings: M = C, so no file is eliminated and the backend narrows. */
    public static final String REASON_NO_POSTINGS = "no_postings";

    /** The hint reason a manifest with no usable postings for THIS plan carries. Decided in {@link #bind}. */
    private static final String HINTS_NO_POSTINGS = "no usable postings for this predicate";
    /**
     * The one reason class manifest v3's ordinal form added: a file whose {@code granularity} is missing,
     * non-numeric, fractional or non-positive names no rows at all, whatever its granule list says.
     */
    private static final String NO_GRANULARITY = "a file declares no usable granularity";

    private static final class Covered {
        final long size;
        final long rows;
        Covered(long size, long rows) {
            this.size = size;
            this.rows = rows;
        }
    }

    private final boolean active;                 // a manifest for THIS snapshot was loaded and parsed
    private final long snapshotId;
    private final String column;
    // key (path under data/) -> identity; slice 2g. Slice 5: this is the SHARED, immutable map of the cached parse
    // -- read only, never copied per plan and never written after construction.
    private Map<String, Covered> files = Collections.emptyMap();
    private final Set<String> matching = new HashSet<>();         // basenames in M (null postings -> M = C)
    private final boolean hasPostings;
    private final boolean predicateUsable;        // EQ / IN literal(s) on `column` were found
    private int manifestKeyType = 0;              // zero denotes a retained v1 manifest
    private int manifestFieldId = -1;
    // slice 5: which path served this plan's manifest -- RapManifestCache.Outcome, rendered as `cache=` in EXPLAIN
    private String cache = RapManifestCache.Outcome.NONE.label();

    // R7: per-file candidate row ranges for THIS plan's predicate, keyed the same way as `files`. Non-empty only for
    // a v3 manifest whose `granules` section validated at parse time AND whose file set agrees with this plan's
    // elimination candidates. Half-open [start, end) absolute row positions within the data FILE -- not within a
    // split -- because that is what the backend's GroupReader intersects into each row group
    // (be/src/formats/parquet/group_reader.cpp). The arrays are the cached parse's own: read, never written.
    private final Map<String, long[][]> hintRanges = new HashMap<>();
    private boolean hintsUsable = false;
    private String hintReason = "no ranged manifest";
    private int hinted = 0;
    private int hintedRanges = 0;

    // K2 / R9: WHY this plan fell back, decided at LOAD time and therefore real when EXPLAIN renders -- unlike the
    // four counters below it, which are incremented later, during scheduling. A single token, no spaces, appended to
    // the existing RAP MANIFEST line so the runners that parse the line's earlier fields are unaffected.
    private String reason = REASON_OK;

    // counters, readable in EXPLAIN and logs
    private int consulted = 0;
    private int covered = 0;
    private int identityMismatch = 0;
    private int dropped = 0;

    private RapCoverage(boolean active, long snapshotId, String column, boolean hasPostings, boolean predicateUsable) {
        this.active = active;
        this.snapshotId = snapshotId;
        this.column = column;
        this.hasPostings = hasPostings;
        this.predicateUsable = predicateUsable;
    }

    /** A coverage that keeps everything, with no reason recorded. Prefer {@link #disabled(long, String)}. */
    public static RapCoverage disabled(long snapshotId) {
        return disabled(snapshotId, REASON_DISABLED);
    }

    /**
     * A coverage that keeps everything, and says why. K2 / R9: the fallback used to be silent -- an operator saw
     * `RAP MANIFEST: off` and had no way to tell a missing manifest from an oversize or a stale one. The reason is
     * decided here, at LOAD time, so {@link #explain()} can render it truthfully; the four counters on the same line
     * cannot be, because they are incremented later, while files are enumerated
     * (see fe-coverage-counters-are-always-zero.md). It is also logged, once per plan, for the same operator.
     */
    public static RapCoverage disabled(long snapshotId, String reason) {
        RapCoverage c = new RapCoverage(false, snapshotId, "", false, false);
        c.reason = reason;
        c.hintsUsable = false;
        c.hintReason = reason;
        if (!REASON_DISABLED.equals(reason) && !REASON_NO_SNAPSHOT.equals(reason)) {
            LOG.info("RAP coverage off for snapshot {}: {} -- ordinary scan", snapshotId, reason);
        }
        return c;
    }

    /**
     * Load the manifest for (table, snapshot) through the table's own FileIO. Never throws: any failure
     * returns a disabled coverage and logs once per query.
     */
    public static RapCoverage load(Table nativeTable, Optional<Long> snapshotId, ScalarOperator predicate) {
        if (nativeTable == null) {
            return disabled(snapshotId != null && snapshotId.isPresent() ? snapshotId.get() : -1L, REASON_NO_TABLE);
        }
        String uuid = nativeTable instanceof BaseTable ? ((BaseTable) nativeTable).operations().current().uuid() : null;
        RapCoverage coverage = load(Config.rap_manifest_dir, nativeTable.io(), uuid, snapshotId, predicate);
        if (coverage.active && coverage.manifestKeyType != 0) {
            // A same-name replacement column must not inherit the old field's postings.
            try {
                org.apache.iceberg.types.Types.NestedField field = nativeTable.schema().findField(coverage.column);
                if (field == null || field.fieldId() != coverage.manifestFieldId
                        || icebergKeyType(field.type()) != coverage.manifestKeyType) {
                    return disabled(coverage.snapshotId, REASON_STALE_COLUMN).withCache(coverage.cache);
                }
            } catch (Exception e) {
                return disabled(coverage.snapshotId, REASON_STALE_COLUMN).withCache(coverage.cache);
            }
        }
        return coverage;
    }

    private RapCoverage withCache(String outcome) {
        this.cache = outcome;
        return this;
    }

    private static int icebergKeyType(org.apache.iceberg.types.Type type) {
        switch (type.typeId()) {
            case STRING:
                return 1;
            case INTEGER:
            case LONG:
                return 2;
            case BOOLEAN:
                return 3;
            case DATE:
                return 4;
            case TIMESTAMP:
                return ((org.apache.iceberg.types.Types.TimestampType) type).shouldAdjustToUTC() ? 0 : 5;
            default:
                return 0;
        }
    }

    /**
     * The loader proper (astra CX-32: testable with any FileIO). An empty directory is "off": nothing is
     * read and every file is kept. Otherwise the manifest for (uuid, snapshot) is read through `io`.
     *
     * <p>Slice 5: the PARSE is kept between plans by {@link RapManifestCache} and only the predicate binding is
     * per plan. The object is still probed on every plan by default ({@code rap_manifest_cache_ttl_ms = 0}) --
     * {@code newInputFile} + {@code exists()} + {@code getLength()}, the same calls this loader already made --
     * and the parse is reused only while that probe's stamp is unchanged, so a same-snapshot republish is seen
     * by the next plan exactly as it was before the cache existed. Absent, oversize and unparsable manifests
     * are never stored and invalidate whatever was held for the same path.
     */
    public static RapCoverage load(String dir, FileIO io, String uuid, Optional<Long> snapshotId, ScalarOperator predicate) {
        if (dir == null || dir.isEmpty() || io == null || snapshotId == null || !snapshotId.isPresent()) {
            boolean off = dir == null || dir.isEmpty() || io == null;
            return disabled(snapshotId != null && snapshotId.isPresent() ? snapshotId.get() : -1L,
                    off ? REASON_DISABLED : REASON_NO_SNAPSHOT);
        }
        long snap = snapshotId.get();
        String path = null;
        try {
            if (uuid == null || uuid.isEmpty()) {
                return disabled(snap, REASON_NO_TABLE_UUID);
            }
            path = dir + (dir.endsWith("/") ? "" : "/") + uuid + "/" + snap + SUFFIX;
            RapManifestCache.Lookup look = RapManifestCache.lookup(path);
            if (look.freshWithoutProbe()) {
                // inside the TTL: the object is not touched at all, which is the only setting that costs no
                // remote call. Off by default, because nothing signals this frontend when a manifest is replaced.
                return served(look.parsed(), predicate, RapManifestCache.Outcome.HIT, uuid, snap, null, 0L);
            }
            InputFile in = io.newInputFile(path);
            if (!in.exists()) {
                RapManifestCache.invalidate(path);
                LOG.info("RAP manifest absent for {} snapshot {} ({}) -- ordinary scan", uuid, snap, path);
                return disabled(snap, REASON_ABSENT);
            }
            // slice 4 (PRD-02): the byte ceiling is applied BEFORE the read; an oversize manifest is "off", never a
            // partial parse
            RapManifestCache.Stamp stamp = RapManifestCache.stampOf(in);
            long length = stamp.length();
            if (length > Config.rap_manifest_max_bytes) {
                RapManifestCache.invalidate(path);
                LOG.warn("RAP manifest for {} snapshot {} is {} bytes, above rap_manifest_max_bytes {} -- ordinary scan",
                        uuid, snap, length, Config.rap_manifest_max_bytes);
                return disabled(snap, REASON_OVERSIZE);
            }
            if (look.matches(stamp)) {
                // the object has not moved since it was parsed: no body read, no parse
                RapManifestCache.confirm(path);
                return served(look.parsed(), predicate, RapManifestCache.Outcome.HIT, uuid, snap, stamp, length);
            }
            String json;
            try (InputStream s = in.newStream()) {
                json = new String(s.readAllBytes(), StandardCharsets.UTF_8);
            }
            RapManifestCache.Outcome outcome = !RapManifestCache.enabled() ? RapManifestCache.Outcome.DISABLED
                    : look.present() ? RapManifestCache.Outcome.REFRESH : RapManifestCache.Outcome.MISS;
            ParseResult result = parseManifest(json, uuid, snap);
            if (result.parsed() == null) {
                // a manifest that does not parse is never cached as valid, and never leaves the previous one serving
                RapManifestCache.invalidate(path);
                RapManifestCache.count(outcome);
                return disabled(snap, result.reason()).withCache(outcome.label());
            }
            RapManifestCache.store(path, result.parsed(), stamp, length);
            return served(result.parsed(), predicate, outcome, uuid, snap, stamp, length);
        } catch (Exception e) {
            if (path != null) {
                RapManifestCache.invalidate(path);
            }
            LOG.warn("RAP manifest unusable for snapshot {}: {} -- ordinary scan", snap, e.toString());
            return disabled(snap, REASON_UNREADABLE);
        }
    }

    /** Bind a parsed manifest to this plan's predicate, and record which path served it. */
    private static RapCoverage served(Parsed parsed, ScalarOperator predicate, RapManifestCache.Outcome outcome,
                                      String uuid, long snap, RapManifestCache.Stamp stamp, long bytes) {
        RapManifestCache.count(outcome);
        RapManifestCache.log(outcome, uuid, snap, stamp, bytes);
        return bind(parsed, predicate).withCache(outcome.label());
    }

    /**
     * Parse a manifest and bind it to the expected (table uuid, snapshot) and the predicate. A manifest
     * for another table or snapshot, or an unknown version, yields a disabled coverage.
     */
    public static RapCoverage fromJson(String json, String expectUuid, long expectSnapshot, ScalarOperator predicate) {
        ParseResult result = parseManifest(json, expectUuid, expectSnapshot);
        return result.parsed() == null ? disabled(expectSnapshot, result.reason()) : bind(result.parsed(), predicate);
    }

    /**
     * Slice 5: everything a manifest establishes BEFORE the query's literals are looked at -- the whole JSON
     * tokenisation, the table and snapshot identity, the file identities, the validation of every posting index
     * and (v3) the validation of every row range. None of it depends on the predicate, so it is done once and kept
     * by {@link RapManifestCache}; {@link #bind} is what each plan pays. Immutable: `files`, `names`, `postings`,
     * `typed`, `ranged` and `nullRanges` are never written after construction and are SHARED with every coverage
     * bound from this manifest.
     */
    static final class Parsed {
        private final int version;
        private final long snapshotId;
        private final String column;
        private final int keyType;
        private final int fieldId;
        private final boolean hasPostings;
        private final String[] names;                   // file index -> key, the order the manifest declares
        private final Map<String, Covered> files;       // key -> identity
        private final Map<String, int[]> postings;      // v1: value -> file indices; null for a typed manifest
        private final Typed typed;                      // v2 / v3: validated typed postings; null for a v1 manifest
        // R7, slice 5: the validated `granules` / `null_granules` sections of a v3 manifest, already turned into
        // value -> file ordinal -> [[start, end), ...]. Null when this manifest carries none, or when its granules
        // were REFUSED -- a refusal
        // is a property of the manifest, not of the query, so it is decided once here and cached with everything
        // else, and `rangeReason` is what every plan bound from it then reports.
        private final NavigableMap<String, Map<Integer, long[][]>> ranged;
        private final Map<Integer, long[][]> nullRanges;
        private final String rangeReason;

        private Parsed(int version, long snapshotId, String column, int keyType, int fieldId, boolean hasPostings,
                       String[] names, Map<String, Covered> files, Map<String, int[]> postings, Typed typed,
                       NavigableMap<String, Map<Integer, long[][]>> ranged, Map<Integer, long[][]> nullRanges,
                       String rangeReason) {
            this.version = version;
            this.snapshotId = snapshotId;
            this.column = column;
            this.keyType = keyType;
            this.fieldId = fieldId;
            this.hasPostings = hasPostings;
            this.names = names;
            this.files = files;
            this.postings = postings;
            this.typed = typed;
            this.ranged = ranged;
            this.nullRanges = nullRanges;
            this.rangeReason = rangeReason;
        }

        int fileCount() {
            return names.length;
        }

        String column() {
            return column;
        }
    }

    /** The predicate-independent half of a typed (v2 / v3) manifest's postings, validated once. */
    private static final class Typed {
        private final int keyType;
        private final TreeMap<String, Set<Integer>> postings;
        private final Set<Integer> nullFiles;

        private Typed(int keyType, TreeMap<String, Set<Integer>> postings, Set<Integer> nullFiles) {
            this.keyType = keyType;
            this.postings = postings;
            this.nullFiles = nullFiles;
        }
    }

    /**
     * The outcome of a predicate-independent parse: the manifest, or the single-token reason it was refused.
     * A refusal is never cached -- {@link #load} invalidates the path -- and the reason is what {@link #explain()}
     * renders, so routing the parse through the cache cannot lose it.
     */
    static final class ParseResult {
        private final Parsed parsed;
        private final String reason;

        private ParseResult(Parsed parsed, String reason) {
            this.parsed = parsed;
            this.reason = reason;
        }

        static ParseResult of(Parsed parsed) {
            return new ParseResult(parsed, REASON_OK);
        }

        static ParseResult refused(String reason) {
            return new ParseResult(null, reason);
        }

        Parsed parsed() {
            return parsed;
        }

        String reason() {
            return reason;
        }
    }

    /**
     * Parse and validate a manifest against the expected (table uuid, snapshot). Returns a refusal -- and logs the
     * reason -- for anything that must keep the ordinary scan: an unknown version, another table or snapshot,
     * a duplicate file name, or any malformed posting. A refusal is never cached.
     */
    static ParseResult parseManifest(String json, String expectUuid, long expectSnapshot) {
        try {
            // Gson's object model otherwise silently keeps the last duplicate posting field.
            try (JsonReader reader = new JsonReader(new StringReader(json))) {
                validateJsonFields(reader, 0);
                if (reader.peek() != JsonToken.END_DOCUMENT) {
                    throw new IllegalArgumentException("trailing RAP manifest content");
                }
            }
            JsonObject m = JsonParser.parseString(json).getAsJsonObject();
            int version = m.has("version") ? m.get("version").getAsBigDecimal().intValueExact() : -1;
            if (version != VERSION && version != TYPED_VERSION && version != RANGED_VERSION) {
                LOG.warn("RAP manifest version unsupported -- ordinary scan");
                return ParseResult.refused(REASON_UNSUPPORTED_VERSION);
            }
            // v3 is v2 plus row ranges: the same typed keys, key type, field id and NULL postings decide
            // elimination, and only the extra `granules` section decides whether plan-time hints are emitted.
            boolean typedManifest = version == TYPED_VERSION || version == RANGED_VERSION;
            if (!m.has("snapshot_id") || m.get("snapshot_id").getAsLong() != expectSnapshot) {
                LOG.warn("RAP manifest is for snapshot {} not {} -- ordinary scan",
                        m.has("snapshot_id") ? m.get("snapshot_id").getAsString() : "?", expectSnapshot);
                return ParseResult.refused(REASON_OTHER_SNAPSHOT);
            }
            // astra CX-29: the table identity is REQUIRED, not optional -- a manifest that does not declare
            // its table, or declares another one, never activates
            if (!m.has("table_uuid") || m.get("table_uuid").isJsonNull() || m.get("table_uuid").getAsString().isEmpty()) {
                LOG.warn("RAP manifest declares no table_uuid -- ordinary scan");
                return ParseResult.refused(REASON_NO_TABLE_UUID);
            }
            if (expectUuid == null || !expectUuid.equals(m.get("table_uuid").getAsString())) {
                LOG.warn("RAP manifest is for table {} not {} -- ordinary scan", m.get("table_uuid").getAsString(), expectUuid);
                return ParseResult.refused(REASON_OTHER_TABLE);
            }
            String column = m.get("column").getAsString();
            Typed typed = typedManifest ? typedPostings(m) : null;
            boolean hasPostings = m.has("postings") && m.get("postings").isJsonObject();
            int keyType = 0;
            int fieldId = -1;
            if (typedManifest) {
                keyType = typed.keyType;
                fieldId = m.get("field_id").getAsBigDecimal().intValueExact();
                if (fieldId <= 0) {
                    return ParseResult.refused(REASON_MALFORMED);
                }
            }
            JsonArray files = m.getAsJsonArray("files");
            List<String> names = new ArrayList<>();
            Map<String, Covered> covered = new HashMap<>();
            for (JsonElement e : files) {
                JsonObject f = e.getAsJsonObject();
                String name = f.get("name").getAsString();
                if (covered.containsKey(name)) {
                    return ParseResult.refused(REASON_MALFORMED);
                }
                names.add(name);
                covered.put(name, new Covered(f.get("size").getAsLong(), f.get("rows").getAsLong()));
            }
            Map<String, int[]> v1Postings = null;
            if (hasPostings) {
                // astra CX-29: EVERY posting is validated before the manifest may eliminate anything -- an
                // index outside `files`, a non-integer, a non-array or an empty list is a malformed manifest,
                // and a malformed manifest keeps every file (never "ignore the bad entry and drop the rest")
                JsonObject postings = m.getAsJsonObject("postings");
                if (version == VERSION) {
                    v1Postings = new HashMap<>();
                }
                for (Map.Entry<String, JsonElement> e : postings.entrySet()) {
                    if (!e.getValue().isJsonArray() || e.getValue().getAsJsonArray().size() == 0) {
                        LOG.warn("RAP manifest posting for a value is not a non-empty array -- ordinary scan");
                        return ParseResult.refused(REASON_MALFORMED);
                    }
                    JsonArray entries = e.getValue().getAsJsonArray();
                    int[] decoded = v1Postings == null ? null : new int[entries.size()];
                    int at = 0;
                    for (JsonElement idx : entries) {
                        // astra CX-29 (second round): Gson's getAsInt() NARROWS -- 0.5, -0.5 and 4294967296 all
                        // become 0 and would pass a post-narrowing bounds check. Integrality and range are decided
                        // on the exact decimal value BEFORE any narrowing.
                        if (!idx.isJsonPrimitive() || !idx.getAsJsonPrimitive().isNumber()) {
                            LOG.warn("RAP manifest posting index is not a number -- ordinary scan");
                            return ParseResult.refused(REASON_MALFORMED);
                        }
                        java.math.BigDecimal exact;
                        try {
                            exact = idx.getAsJsonPrimitive().getAsBigDecimal();
                        } catch (NumberFormatException nfe) {
                            LOG.warn("RAP manifest posting index is not a decimal number -- ordinary scan");
                            return ParseResult.refused(REASON_MALFORMED);
                        }
                        if (exact.stripTrailingZeros().scale() > 0) {
                            LOG.warn("RAP manifest posting index {} is not an integer -- ordinary scan", exact);
                            return ParseResult.refused(REASON_MALFORMED);
                        }
                        if (exact.signum() < 0 || exact.compareTo(java.math.BigDecimal.valueOf(names.size() - 1)) > 0) {
                            LOG.warn("RAP manifest posting index {} outside its {} files -- ordinary scan", exact, names.size());
                            return ParseResult.refused(REASON_MALFORMED);
                        }
                        if (decoded != null) {
                            decoded[at++] = exact.intValueExact();
                        }
                    }
                    if (decoded != null) {
                        v1Postings.put(e.getKey(), decoded);
                    }
                }
            }
            // R7 under slice 5: the `granules` section is validated HERE, with the rest of the document, because
            // nothing about it depends on the query. It never throws and never changes an elimination decision: a
            // defect leaves `ranged` null and records the reason, which is what every plan bound from it reports.
            Ranged ranges = version == RANGED_VERSION && hasPostings
                    ? parseRanges(m, names, covered, expectSnapshot)
                    : Ranged.refused(HINTS_NO_POSTINGS);
            return ParseResult.of(new Parsed(version, expectSnapshot, column, keyType, fieldId, hasPostings,
                    names.toArray(new String[0]), Collections.unmodifiableMap(covered),
                    v1Postings == null ? null : Collections.unmodifiableMap(v1Postings), typed,
                    ranges.ranged, ranges.nullRanges, ranges.reason));
        } catch (Exception e) {
            LOG.warn("RAP manifest malformed ({}) -- ordinary scan", e.getClass().getSimpleName());
            return ParseResult.refused(REASON_MALFORMED);
        }
    }

    /**
     * Slice 5: bind a parsed manifest to ONE plan's predicate. This is all a cached manifest costs per plan --
     * the literals of the query looked up in the postings, the matching file keys collected, and (v3) the row
     * ranges those literals select out of the already-derived `granules`. Everything else was established once
     * in {@link #parseManifest}.
     */
    static RapCoverage bind(Parsed p, ScalarOperator predicate) {
        try {
            Set<Integer> typedCandidates = p.version == VERSION ? null
                    : RapManifestPredicate.matching(predicate, p.column, p.typed.keyType, p.typed.postings,
                            p.typed.nullFiles);
            List<String> literals = p.version == VERSION ? extractLiterals(predicate, p.column) : null;
            RapCoverage c = new RapCoverage(true, p.snapshotId, p.column, p.hasPostings,
                    p.version == VERSION ? literals != null : typedCandidates != null);
            // Active is not the same as useful: say which of the two it is, at load time, so EXPLAIN can render it.
            c.reason = !p.hasPostings ? REASON_NO_POSTINGS : (c.predicateUsable ? REASON_OK : REASON_PREDICATE_NOT_BOUND);
            c.manifestKeyType = p.keyType;
            c.manifestFieldId = p.fieldId;
            c.files = p.files;
            if (p.hasPostings) {
                if (literals != null && p.postings != null) {
                    for (String lit : literals) {
                        int[] indices = p.postings.get(lit);
                        if (indices == null) {
                            continue;
                        }
                        for (int index : indices) {
                            // every index was proven an exact integer within [0, files) at parse time
                            c.matching.add(p.names[index]);
                        }
                    }
                }
                if (typedCandidates != null) {
                    for (int index : typedCandidates) {
                        c.matching.add(p.names[index]);
                    }
                }
            }
            if (p.version == RANGED_VERSION) {
                // R7: never throws, never changes an elimination decision -- it only decides whether this plan
                // may also ship row ranges, and records why when it may not.
                c.bindRowRanges(p, predicate, typedCandidates);
            }
            return c;
        } catch (Exception e) {
            LOG.warn("RAP manifest malformed ({}) -- ordinary scan", e.getClass().getSimpleName());
            return disabled(p.snapshotId, REASON_MALFORMED);
        }
    }

    private static void validateJsonFields(JsonReader reader, int depth) throws IOException {
        if (depth > 32) {
            throw new IllegalArgumentException("RAP manifest nesting limit");
        }
        if (reader.peek() == JsonToken.BEGIN_OBJECT) {
            reader.beginObject();
            Set<String> fields = new HashSet<>();
            while (reader.hasNext()) {
                if (!fields.add(reader.nextName())) {
                    throw new IllegalArgumentException("duplicate RAP manifest field");
                }
                validateJsonFields(reader, depth + 1);
            }
            reader.endObject();
        } else if (reader.peek() == JsonToken.BEGIN_ARRAY) {
            reader.beginArray();
            while (reader.hasNext()) {
                validateJsonFields(reader, depth + 1);
            }
            reader.endArray();
        } else {
            reader.skipValue();
        }
    }

    private static Set<Integer> fileIndices(JsonElement entry, int size, boolean allowEmpty) {
        if (entry == null || !entry.isJsonArray() || (!allowEmpty && entry.getAsJsonArray().isEmpty())) {
            throw new IllegalArgumentException("invalid RAP file-index array");
        }
        Set<Integer> out = new HashSet<>();
        for (JsonElement value : entry.getAsJsonArray()) {
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("invalid RAP file index");
            }
            int index = value.getAsBigDecimal().intValueExact();
            if (index < 0 || index >= size || !out.add(index)) {
                throw new IllegalArgumentException("invalid or duplicate RAP file index");
            }
        }
        return out;
    }

    /**
     * The predicate-INDEPENDENT half of what a typed (v2 / v3) manifest needs: the key type, and every posting key
     * and file-index set validated. The predicate is applied later, in {@link #bind}, so this survives in the
     * cache across plans. Throws for anything unusable, which {@link #parseManifest} turns into "ordinary scan".
     */
    private static Typed typedPostings(JsonObject manifest) {
        int type = manifest.get("key_type").getAsBigDecimal().intValueExact();
        if (type < 1 || type > 5 || !"hex".equals(manifest.get("key_encoding").getAsString())) {
            throw new IllegalArgumentException("unsupported RAP typed manifest");
        }
        int size = manifest.getAsJsonArray("files").size();
        TreeMap<String, Set<Integer>> postings = new TreeMap<>();
        for (Map.Entry<String, JsonElement> entry : manifest.getAsJsonObject("postings").entrySet()) {
            RapManifestPredicate.validateKey(entry.getKey(), type);
            postings.put(entry.getKey(), fileIndices(entry.getValue(), size, false));
        }
        return new Typed(type, postings, fileIndices(manifest.get("null_postings"), size, true));
    }

    // ---------------------------------------------------------------------------------------------------------
    // R7: plan-time row ranges.
    //
    // The completeness rule is untouched by everything below. Hints may only NARROW the rows read inside a file
    // that `decide` already scheduled; they never add, remove or reorder a file, and a file with no hint is read
    // exactly as it is today. So every failure here -- a missing section, a malformed range, a predicate shape the
    // range index cannot answer, a disagreement with the elimination candidates -- turns the hints off and leaves
    // the scan correct and complete, with the reason recorded.
    //
    // Slice 5 splits this work the way the rest of the manifest is split: the SECTION is parsed, validated and
    // turned from granule ordinals into row ranges once, in `parseRanges` (nothing about it depends on the query,
    // so a cache hit re-derives none of it), and only the SELECTION -- which of those ranges this plan's literals
    // choose -- happens per plan, in `bindRowRanges`.
    // ---------------------------------------------------------------------------------------------------------

    /** The row ranges the `granules` / `null_granules` sections name, or the reason they were refused. Cached. */
    private static final class Ranged {
        private final NavigableMap<String, Map<Integer, long[][]>> ranged;
        private final Map<Integer, long[][]> nullRanges;
        private final String reason;

        private Ranged(NavigableMap<String, Map<Integer, long[][]>> ranged, Map<Integer, long[][]> nullRanges,
                       String reason) {
            this.ranged = ranged;
            this.nullRanges = nullRanges;
            this.reason = reason;
        }

        static Ranged of(NavigableMap<String, Map<Integer, long[][]>> ranged, Map<Integer, long[][]> nullRanges) {
            return new Ranged(ranged, nullRanges, "");
        }

        static Ranged refused(String reason) {
            return new Ranged(null, null, reason);
        }
    }

    /**
     * Validate a v3 manifest's `granules` and `null_granules` against the postings they must agree with, and derive
     * the row ranges they name. Never throws: a defect returns a refusal carrying the reason, which is cached with
     * the parse and reported by every plan bound from it. This runs ONCE per manifest, not once per plan.
     */
    private static Ranged parseRanges(JsonObject m, List<String> names, Map<String, Covered> covered, long snapshot) {
        try {
            // Before a single hint: every file must say what a granule ordinal MEANS for it.
            long[] granularity = granularities(m.getAsJsonArray("files"));
            JsonObject postings = m.getAsJsonObject("postings");
            if (!m.has("granules") || !m.get("granules").isJsonObject()) {
                throw new IllegalArgumentException("v3 manifest without a granules object");
            }
            JsonObject granules = m.getAsJsonObject("granules");
            if (granules.size() != postings.size()) {
                throw new IllegalArgumentException("granules and postings name different values");
            }
            NavigableMap<String, Map<Integer, long[][]>> ranged = new TreeMap<>();
            for (Map.Entry<String, JsonElement> entry : postings.entrySet()) {
                JsonElement byFile = granules.get(entry.getKey());
                if (byFile == null || !byFile.isJsonObject()) {
                    throw new IllegalArgumentException("a posting value has no granules object");
                }
                ranged.put(entry.getKey(), Collections.unmodifiableMap(perFileRanges(byFile.getAsJsonObject(),
                        ordinalsOf(entry.getValue()), names, covered, granularity)));
            }
            if (!m.has("null_granules") || !m.get("null_granules").isJsonObject()) {
                throw new IllegalArgumentException("v3 manifest without a null_granules object");
            }
            Map<Integer, long[][]> nullRanges = perFileRanges(m.getAsJsonObject("null_granules"),
                    ordinalsOf(m.get("null_postings")), names, covered, granularity);
            return Ranged.of(Collections.unmodifiableNavigableMap(ranged), Collections.unmodifiableMap(nullRanges));
        } catch (Exception e) {
            String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            LOG.warn("RAP manifest for snapshot {} carries no usable row ranges ({}) -- files are still eliminated, "
                    + "and scheduled files are narrowed by the backend sidecar as before", snapshot, reason);
            return Ranged.refused(reason);
        }
    }

    /**
     * Select this plan's row ranges out of the manifest's already-validated ones. Predicate-dependent, so it runs
     * per plan; everything it reads was established once, at parse time, and is shared read-only.
     */
    private void bindRowRanges(Parsed p, ScalarOperator predicate, Set<Integer> candidates) {
        if (!Config.rap_plan_row_range_hints) {
            // a mutable config, so it is honoured per plan and never baked into the cached parse
            hintReason = "disabled by rap_plan_row_range_hints";
            return;
        }
        if (!hasPostings || !predicateUsable || candidates == null) {
            hintReason = HINTS_NO_POSTINGS;
            return;
        }
        if (p.ranged == null) {
            hintReason = p.rangeReason;   // the parse-time defect, decided once and cached with the manifest
            return;
        }
        try {
            Map<Integer, long[][]> selected =
                    RapManifestPredicate.matchingRanges(predicate, p.column, p.keyType, p.ranged, p.nullRanges);
            if (selected == null) {
                throw new IllegalArgumentException("predicate shape carries no row ranges");
            }
            // The guard that keeps hints and elimination from ever drifting apart: the files the range index
            // selects must be exactly the files the posting index selected. A divergence is a manifest defect,
            // and it turns hints off rather than narrowing a read the completeness rule did not sanction.
            if (!selected.keySet().equals(candidates)) {
                throw new IllegalArgumentException("range candidates differ from posting candidates");
            }
            for (Map.Entry<Integer, long[][]> entry : selected.entrySet()) {
                // A zero-length intersection means the conjunction matches no row of that file. The posting rule
                // still schedules it (it is in M), so emit no hint and let the ordinary scan settle it.
                if (entry.getValue().length > 0) {
                    hintRanges.put(p.names[entry.getKey()], entry.getValue());
                }
            }
            hintsUsable = true;
            hintReason = "";
        } catch (Exception e) {
            hintRanges.clear();
            hintsUsable = false;
            hintReason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            LOG.warn("RAP manifest for snapshot {} carries no usable row ranges ({}) -- files are still eliminated, "
                    + "and scheduled files are narrowed by the backend sidecar as before", snapshotId, hintReason);
        }
    }

    /** The file ordinals of one posting entry; every index was proven an exact integer within [0, files) already. */
    private static Set<Integer> ordinalsOf(JsonElement postingArray) {
        Set<Integer> out = new HashSet<>();
        if (postingArray == null || !postingArray.isJsonArray()) {
            return out;
        }
        for (JsonElement idx : postingArray.getAsJsonArray()) {
            out.add(idx.getAsJsonPrimitive().getAsBigDecimal().intValueExact());
        }
        return out;
    }

    /**
     * Every file's granularity, read PER FILE. The granularity is what turns an ordinal back into rows, so it is
     * taken from the same record that declares that file's row count rather than from one number for the whole
     * manifest: a manifest that ever mixes granularities would otherwise be silently wrong, and the top-level
     * {@code granularity_rows} is a v1/v2 field this frontend has never validated. Missing, non-numeric,
     * fractional and non-positive are one failure with one reason -- in every case an ordinal cannot be turned
     * back into rows -- and it is raised before a single hint is derived.
     */
    private static long[] granularities(JsonArray files) {
        long[] out = new long[files.size()];
        for (int i = 0; i < files.size(); i++) {
            JsonElement declared = files.get(i).getAsJsonObject().get("granularity");
            if (declared == null || !declared.isJsonPrimitive() || !declared.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException(NO_GRANULARITY);
            }
            long granularity;
            try {
                granularity = declared.getAsJsonPrimitive().getAsBigDecimal().longValueExact();
            } catch (ArithmeticException | NumberFormatException e) {
                throw new IllegalArgumentException(NO_GRANULARITY);
            }
            if (granularity <= 0) {
                throw new IllegalArgumentException(NO_GRANULARITY);
            }
            out[i] = granularity;
        }
        return out;
    }

    /**
     * {@code {"<file ordinal>": [granule ordinal, ...]}} for one posting value, validated against the file ordinals
     * the posting itself declares. Throws on any defect; the caller turns hints off and keeps the scan complete.
     */
    private static Map<Integer, long[][]> perFileRanges(JsonObject byFile, Set<Integer> expected, List<String> names,
                                                        Map<String, Covered> covered, long[] granularity) {
        Map<Integer, long[][]> out = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : byFile.entrySet()) {
            int ordinal;
            try {
                ordinal = new java.math.BigDecimal(entry.getKey()).intValueExact();
            } catch (NumberFormatException | ArithmeticException e) {
                throw new IllegalArgumentException("granule file ordinal is not an integer");
            }
            if (ordinal < 0 || ordinal >= names.size()) {
                throw new IllegalArgumentException("granule file ordinal outside the manifest files");
            }
            Covered file = covered.get(names.get(ordinal));
            if (file == null) {
                throw new IllegalArgumentException("granule file ordinal names no covered file");
            }
            out.put(ordinal, fileGranules(entry.getValue(), file.rows, granularity[ordinal]));
        }
        if (!out.keySet().equals(expected)) {
            throw new IllegalArgumentException("granules and postings name different files for a value");
        }
        return out;
    }

    /**
     * One file's granule ordinals, as the ROW RANGES they name. Granule {@code g} is
     * {@code [g * gran, min((g + 1) * gran, rows))} -- the file's last granule is short and is clipped to
     * {@code rows} -- and adjacent granules are coalesced, so the result is the non-empty, ascending, disjoint,
     * non-touching list the rest of the frontend and the backend already expect, and is identical to the list the
     * sidecar itself holds.
     *
     * <p>The list must be non-empty, and every ordinal exactly integral, ascending, unique and inside
     * [0, ceil(rows / gran)). Those four are what a row-pair manifest spent six checks on: a negative or
     * out-of-range ordinal is the range that ran past the file, and an unordered or repeated one is the overlap.
     */
    private static long[][] fileGranules(JsonElement element, long rows, long gran) {
        if (element == null || !element.isJsonArray() || element.getAsJsonArray().isEmpty()) {
            throw new IllegalArgumentException("a file's granule list is absent or empty");
        }
        JsonArray array = element.getAsJsonArray();
        // rounded UP, and written without `rows + gran` so a huge declared row count cannot overflow
        long count = rows / gran + (rows % gran == 0 ? 0 : 1);
        long[][] out = new long[array.size()][];
        int at = 0;
        long previous = -1;
        for (int i = 0; i < array.size(); i++) {
            long ordinal = exactOrdinal(array.get(i));
            if (ordinal < 0 || ordinal >= count) {
                throw new IllegalArgumentException("a granule ordinal is outside the file's granules");
            }
            if (ordinal <= previous) {
                throw new IllegalArgumentException("granule ordinals are unordered or repeated");
            }
            previous = ordinal;
            long start = ordinal * gran;
            // the last granule is clipped to the file's rows; every other one is exactly `gran` long
            long end = ordinal == count - 1 ? rows : start + gran;
            if (at > 0 && out[at - 1][1] == start) {
                // Adjacent granules are ONE range. Not observable in a hint: every matchingRanges path ends in
                // RapManifestPredicate.unionRanges -> mergeRanges, which joins touching intervals anyway, and a
                // source-reversion mutant of these four lines is the one mutant of this change that survives the
                // suite. It is here for what the CACHE holds: `Parsed.ranged` is kept between plans, and on the
                // 1,500-file density fixture this is 3,410,632 long[2] rather than 9,306,946 -- 2.73x. Do not
                // "simplify" it away on the grounds that no test covers it.
                out[at - 1][1] = end;
            } else {
                out[at++] = new long[] {start, end};
            }
        }
        return at == array.size() ? out : java.util.Arrays.copyOf(out, at);
    }

    private static long exactOrdinal(JsonElement element) {
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException("a granule ordinal is not a number");
        }
        // Gson narrows silently: 0.5 and 2^64 both become 0 through getAsLong(). Decide on the exact decimal, and
        // name the defect rather than letting BigDecimal's "Rounding necessary" stand as the recorded reason.
        try {
            return element.getAsJsonPrimitive().getAsBigDecimal().longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException("a granule ordinal is not an exact integer");
        }
    }

    /**
     * The row ranges to ship on this file's scan range(s), or null when this plan emits no hint for it.
     *
     * <p>File-relative and split-independent by construction: the positions are absolute within the data FILE, so
     * every scan range of a split file carries the same list and the backend intersects it with each row group it
     * actually reads (GroupReader::_apply_selected_row_ranges). A file gets a hint only when it is covered, its
     * identity still matches the manifest, and the manifest's ranges bound to this predicate -- the same three
     * conditions under which {@code decide} was willing to reason about it at all.
     */
    public long[][] rowRangesFor(DataFile file) {
        if (!active || !hintsUsable || file == null) {
            return null;
        }
        String key = keyOf(file.location());
        Covered c = files.get(key);
        if (c == null || c.size != file.fileSizeInBytes() || c.rows != file.recordCount()) {
            return null; // uncovered, or rewritten under the same name: P - C is scanned whole
        }
        long[][] ranges = hintRanges.get(key);
        if (ranges == null || ranges.length == 0) {
            return null;
        }
        hinted++;
        hintedRanges += ranges.length;
        return ranges;
    }

    /**
     * EQ / IN string literals on `column` found directly under the AND-tree of `predicate`; null when
     * there is none (or a NOT IN), in which case the FE eliminates nothing.
     */
    static List<String> extractLiterals(ScalarOperator predicate, String column) {
        if (predicate == null) {
            return null;
        }
        List<String> out = new ArrayList<>();
        collect(predicate, column, out);
        return out.isEmpty() ? null : out;
    }

    private static void collect(ScalarOperator op, String column, List<String> out) {
        if (op instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator c = (CompoundPredicateOperator) op;
            if (c.isAnd()) {
                for (ScalarOperator child : c.getChildren()) {
                    collect(child, column, out);
                }
            }
            return;
        }
        if (op instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator b = (BinaryPredicateOperator) op;
            if (b.getBinaryType() != BinaryType.EQ) {
                return;
            }
            String lit = literalOn(b.getChild(0), b.getChild(1), column);
            if (lit == null) {
                lit = literalOn(b.getChild(1), b.getChild(0), column);
            }
            if (lit != null) {
                out.add(lit);
            }
            return;
        }
        if (op instanceof InPredicateOperator) {
            InPredicateOperator in = (InPredicateOperator) op;
            if (in.isNotIn() || !(in.getChild(0) instanceof ColumnRefOperator)) {
                return;
            }
            if (!column.equalsIgnoreCase(((ColumnRefOperator) in.getChild(0)).getName())) {
                return;
            }
            if (RapManifestPredicate.keyType(in.getChild(0).getType()) != 1) {
                return;
            }
            List<String> values = new ArrayList<>();
            for (int i = 1; i < in.getChildren().size(); i++) {
                ScalarOperator v = in.getChild(i);
                if (!(v instanceof ConstantOperator) || ((ConstantOperator) v).isNull()
                        || RapManifestPredicate.keyType(v.getType()) != 1) {
                    return; // a non-constant or NULL member: the FE cannot reason about this IN
                }
                values.add(((ConstantOperator) v).getVarchar());
            }
            out.addAll(values);
        }
    }

    private static String literalOn(ScalarOperator col, ScalarOperator val, String column) {
        if (col instanceof ColumnRefOperator && val instanceof ConstantOperator && !((ConstantOperator) val).isNull()
                && column.equalsIgnoreCase(((ColumnRefOperator) col).getName())
                && RapManifestPredicate.keyType(col.getType()) == 1 && RapManifestPredicate.keyType(val.getType()) == 1) {
            return ((ConstantOperator) val).getVarchar();
        }
        return null;
    }

    /**
     * slice 2g v4 (fork production-readiness review PRD-01; m37 and m38 reviews): the key of a data file keeps its
     * STORAGE NAMESPACE -- "scheme://rest" becomes "scheme/rest" with rest's leading slashes dropped; a location
     * without a scheme, and a file:// URI, are the local filesystem, "file/rest". Bucket, table location, partition
     * directories and file name all stay. v2's "path after the last /data/" dropped the table, so two tables with the
     * same suffix, size and row count could share a sidecar; v3 dropped the scheme, so gs://b/... and s3://b/... (one
     * bucket name in two stores) shared one. One rule, no fallback. The BE's RapIndex::key_of and the harness's
     * rap_index_build.key_of apply the same rule.
     */
    public static String keyOf(String loc) {
        int sch = loc.indexOf("://");
        String scheme = sch >= 0 ? loc.substring(0, sch) : "";
        String p = sch >= 0 ? loc.substring(sch + 3) : loc;
        if (scheme.isEmpty()) {
            scheme = "file";
        }
        // slice 2g v4b (m39 review): a relative local path names a file only together with the process's working
        // directory, and under the plain rule it took the same key as the absolute path with the same spelling.
        // Resolve it; do not normalise, since a lexical rewrite across a symlink would name a different file.
        if ("file".equals(scheme) && !p.isEmpty() && !p.startsWith("/")) {
            p = java.nio.file.Paths.get("").toAbsolutePath() + "/" + p;
        }
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        return scheme + "/" + p;
    }

    /** The completeness rule for one planned data file. */
    public Decision decide(DataFile file) {
        consulted++;
        if (!active || file == null) {
            return Decision.KEEP;
        }
        String base = keyOf(file.location()); // slice 2g: the path under data/, not the basename
        Covered c = files.get(base);
        if (c == null) {
            return Decision.KEEP; // uncovered: P − C is always scanned
        }
        if (c.size != file.fileSizeInBytes() || c.rows != file.recordCount()) {
            identityMismatch++;
            return Decision.KEEP; // a rewritten or different file under the same name: treat as uncovered
        }
        covered++;
        if (!hasPostings || !predicateUsable) {
            return Decision.KEEP; // M = C: the BE narrows inside the file
        }
        if (matching.contains(base)) {
            return Decision.KEEP;
        }
        dropped++;
        return Decision.DROP;
    }

    public boolean isActive() {
        return active;
    }

    /**
     * The plan line. {@code state}, {@code snapshot}, {@code column}, {@code postings}, {@code reason},
     * {@code hints} and {@code cache} describe the manifest that was LOADED and are real here. The four counters
     * are incremented later, by {@code decide} during scheduling, so they are structurally zero at render time and
     * always have been -- see fe-coverage-counters-are-always-zero.md; they are kept because runners parse them,
     * and the fields that answer "did the manifest do anything" are {@code reason} and, in the query profile,
     * {@code ScanRanges}. The new fields are APPENDED so those runners' patterns still match, and {@code cache} is
     * appended LAST: every retained parser of this line matches its fields by name from the front, and
     * harness/visibility_check.py reads {@code cache=} as the trailing field.
     */
    public String explain() {
        return String.format("RAP MANIFEST: %s snapshot=%d column=%s postings=%s consulted=%d covered=%d "
                        + "identity_mismatch=%d dropped=%d reason=%s hints=%s cache=%s",
                active ? "active" : "off", snapshotId, column, hasPostings ? "yes" : "no",
                consulted, covered, identityMismatch, dropped, reason, hintsUsable ? "yes" : "no", cache);
    }

    /** Why this plan fell back, or {@link #REASON_OK}. Decided at load time, so it is real when EXPLAIN renders. */
    public String getReason() {
        return reason;
    }

    /** Which path served this plan's manifest: none, disabled, miss, hit or refresh. */
    public String getCache() {
        return cache;
    }

    public int getConsulted() {
        return consulted;
    }

    public int getCovered() {
        return covered;
    }

    public int getIdentityMismatch() {
        return identityMismatch;
    }

    public int getDropped() {
        return dropped;
    }

    /** R7: true when this plan may ship row ranges (a v3 manifest whose ranges bound to this predicate). */
    public boolean hasRowRangeHints() {
        return hintsUsable;
    }

    /** Empty when hints are usable; otherwise why this plan ships none. Decided at LOAD time, so it is real. */
    public String getHintReason() {
        return hintReason;
    }

    /** Files this plan hinted, and the row ranges shipped. Decided at SCHEDULING time -- zero while EXPLAIN renders. */
    public int getHinted() {
        return hinted;
    }

    public int getHintedRanges() {
        return hintedRanges;
    }
}
