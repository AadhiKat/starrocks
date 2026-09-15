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
 */
public class RapCoverage {
    private static final Logger LOG = LogManager.getLogger(RapCoverage.class);
    public static final int VERSION = 1;
    public static final int TYPED_VERSION = 2;
    public static final String SUFFIX = ".rapm.json";

    public enum Decision { KEEP, DROP }

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

    /** A coverage that keeps everything. */
    public static RapCoverage disabled(long snapshotId) {
        return new RapCoverage(false, snapshotId, "", false, false);
    }

    /**
     * Load the manifest for (table, snapshot) through the table's own FileIO. Never throws: any failure
     * returns a disabled coverage and logs once per query.
     */
    public static RapCoverage load(Table nativeTable, Optional<Long> snapshotId, ScalarOperator predicate) {
        if (nativeTable == null) {
            return disabled(snapshotId != null && snapshotId.isPresent() ? snapshotId.get() : -1L);
        }
        String uuid = nativeTable instanceof BaseTable ? ((BaseTable) nativeTable).operations().current().uuid() : null;
        RapCoverage coverage = load(Config.rap_manifest_dir, nativeTable.io(), uuid, snapshotId, predicate);
        if (coverage.active && coverage.manifestKeyType != 0) {
            // A same-name replacement column must not inherit the old field's postings.
            try {
                org.apache.iceberg.types.Types.NestedField field = nativeTable.schema().findField(coverage.column);
                if (field == null || field.fieldId() != coverage.manifestFieldId
                        || icebergKeyType(field.type()) != coverage.manifestKeyType) {
                    return disabled(coverage.snapshotId).withCache(coverage.cache);
                }
            } catch (Exception e) {
                return disabled(coverage.snapshotId).withCache(coverage.cache);
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
            return disabled(snapshotId != null && snapshotId.isPresent() ? snapshotId.get() : -1L);
        }
        long snap = snapshotId.get();
        String path = null;
        try {
            if (uuid == null || uuid.isEmpty()) {
                return disabled(snap);
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
                return disabled(snap);
            }
            // slice 4 (PRD-02): the byte ceiling is applied BEFORE the read; an oversize manifest is "off", never a
            // partial parse
            RapManifestCache.Stamp stamp = RapManifestCache.stampOf(in);
            long length = stamp.length();
            if (length > Config.rap_manifest_max_bytes) {
                RapManifestCache.invalidate(path);
                LOG.warn("RAP manifest for {} snapshot {} is {} bytes, above rap_manifest_max_bytes {} -- ordinary scan",
                        uuid, snap, length, Config.rap_manifest_max_bytes);
                return disabled(snap);
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
            Parsed parsed = parseManifest(json, uuid, snap);
            if (parsed == null) {
                // a manifest that does not parse is never cached as valid, and never leaves the previous one serving
                RapManifestCache.invalidate(path);
                RapManifestCache.count(outcome);
                return disabled(snap).withCache(outcome.label());
            }
            RapManifestCache.store(path, parsed, stamp, length);
            return served(parsed, predicate, outcome, uuid, snap, stamp, length);
        } catch (Exception e) {
            if (path != null) {
                RapManifestCache.invalidate(path);
            }
            LOG.warn("RAP manifest unusable for snapshot {}: {} -- ordinary scan", snap, e.toString());
            return disabled(snap);
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
        Parsed parsed = parseManifest(json, expectUuid, expectSnapshot);
        return parsed == null ? disabled(expectSnapshot) : bind(parsed, predicate);
    }

    /**
     * Slice 5: everything a manifest establishes BEFORE the query's literals are looked at -- the whole JSON
     * tokenisation, the table and snapshot identity, the file identities and the validation of every posting
     * index. None of it depends on the predicate, so it is done once and kept by {@link RapManifestCache};
     * {@link #bind} is what each plan pays. Immutable: `files`, `names`, `postings` and `typed` are never
     * written after construction and are SHARED with every coverage bound from this manifest.
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
        private final Typed typed;                      // v2: validated typed postings; null for a v1 manifest

        private Parsed(int version, long snapshotId, String column, int keyType, int fieldId, boolean hasPostings,
                       String[] names, Map<String, Covered> files, Map<String, int[]> postings, Typed typed) {
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
        }

        int fileCount() {
            return names.length;
        }

        String column() {
            return column;
        }
    }

    /** The predicate-independent half of a typed (v2) manifest's postings, validated once. */
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
     * Parse and validate a manifest against the expected (table uuid, snapshot). Returns null -- and logs the
     * reason -- for anything that must keep the ordinary scan: an unknown version, another table or snapshot,
     * a duplicate file name, or any malformed posting. A null return is never cached.
     */
    static Parsed parseManifest(String json, String expectUuid, long expectSnapshot) {
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
            if (version != VERSION && version != TYPED_VERSION) {
                LOG.warn("RAP manifest version unsupported -- ordinary scan");
                return null;
            }
            if (!m.has("snapshot_id") || m.get("snapshot_id").getAsLong() != expectSnapshot) {
                LOG.warn("RAP manifest is for snapshot {} not {} -- ordinary scan",
                        m.has("snapshot_id") ? m.get("snapshot_id").getAsString() : "?", expectSnapshot);
                return null;
            }
            // astra CX-29: the table identity is REQUIRED, not optional -- a manifest that does not declare
            // its table, or declares another one, never activates
            if (!m.has("table_uuid") || m.get("table_uuid").isJsonNull() || m.get("table_uuid").getAsString().isEmpty()) {
                LOG.warn("RAP manifest declares no table_uuid -- ordinary scan");
                return null;
            }
            if (expectUuid == null || !expectUuid.equals(m.get("table_uuid").getAsString())) {
                LOG.warn("RAP manifest is for table {} not {} -- ordinary scan", m.get("table_uuid").getAsString(), expectUuid);
                return null;
            }
            String column = m.get("column").getAsString();
            Typed typed = version == TYPED_VERSION ? typedPostings(m) : null;
            boolean hasPostings = m.has("postings") && m.get("postings").isJsonObject();
            int keyType = 0;
            int fieldId = -1;
            if (version == TYPED_VERSION) {
                keyType = typed.keyType;
                fieldId = m.get("field_id").getAsBigDecimal().intValueExact();
                if (fieldId <= 0) {
                    return null;
                }
            }
            JsonArray files = m.getAsJsonArray("files");
            List<String> names = new ArrayList<>();
            Map<String, Covered> covered = new HashMap<>();
            for (JsonElement e : files) {
                JsonObject f = e.getAsJsonObject();
                String name = f.get("name").getAsString();
                if (covered.containsKey(name)) {
                    return null;
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
                        return null;
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
                            return null;
                        }
                        java.math.BigDecimal exact;
                        try {
                            exact = idx.getAsJsonPrimitive().getAsBigDecimal();
                        } catch (NumberFormatException nfe) {
                            LOG.warn("RAP manifest posting index is not a decimal number -- ordinary scan");
                            return null;
                        }
                        if (exact.stripTrailingZeros().scale() > 0) {
                            LOG.warn("RAP manifest posting index {} is not an integer -- ordinary scan", exact);
                            return null;
                        }
                        if (exact.signum() < 0 || exact.compareTo(java.math.BigDecimal.valueOf(names.size() - 1)) > 0) {
                            LOG.warn("RAP manifest posting index {} outside its {} files -- ordinary scan", exact, names.size());
                            return null;
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
            return new Parsed(version, expectSnapshot, column, keyType, fieldId, hasPostings,
                    names.toArray(new String[0]), Collections.unmodifiableMap(covered),
                    v1Postings == null ? null : Collections.unmodifiableMap(v1Postings), typed);
        } catch (Exception e) {
            LOG.warn("RAP manifest malformed ({}) -- ordinary scan", e.getClass().getSimpleName());
            return null;
        }
    }

    /**
     * Slice 5: bind a parsed manifest to ONE plan's predicate. This is all a cached manifest costs per plan --
     * the literals of the query looked up in the postings, and the matching file keys collected. Everything
     * else was established once in {@link #parseManifest}.
     */
    static RapCoverage bind(Parsed p, ScalarOperator predicate) {
        try {
            Set<Integer> typedCandidates = p.version == TYPED_VERSION
                    ? RapManifestPredicate.matching(predicate, p.column, p.typed.keyType, p.typed.postings, p.typed.nullFiles)
                    : null;
            List<String> literals = p.version == VERSION ? extractLiterals(predicate, p.column) : null;
            RapCoverage c = new RapCoverage(true, p.snapshotId, p.column, p.hasPostings,
                    p.version == TYPED_VERSION ? typedCandidates != null : literals != null);
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
            return c;
        } catch (Exception e) {
            LOG.warn("RAP manifest malformed ({}) -- ordinary scan", e.getClass().getSimpleName());
            return disabled(p.snapshotId);
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
     * The predicate-INDEPENDENT half of what a typed (v2) manifest needs: the key type, and every posting key
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
     * The plan's account of the manifest. `consulted`, `covered`, `identity_mismatch` and `dropped` are
     * structurally zero from SQL -- this string is rendered before scheduling calls {@link #decide}
     * (`fe-coverage-counters-are-always-zero.md`). `state`, `snapshot`, `column`, `postings` and `cache` are
     * established when the manifest LOADS, which happens before the render, so those five are real. `cache` is
     * appended last on purpose: every retained parser of this line matches its fields by name from the front.
     */
    public String explain() {
        return String.format("RAP MANIFEST: %s snapshot=%d column=%s postings=%s consulted=%d covered=%d "
                        + "identity_mismatch=%d dropped=%d cache=%s",
                active ? "active" : "off", snapshotId, column, hasPostings ? "yes" : "no",
                consulted, covered, identityMismatch, dropped, cache);
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
}
