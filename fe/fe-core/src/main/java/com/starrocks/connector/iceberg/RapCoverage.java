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
    private final Map<String, Covered> files = new HashMap<>();   // key (path under data/) -> identity; slice 2g
    private final Set<String> matching = new HashSet<>();         // basenames in M (null postings -> M = C)
    private final boolean hasPostings;
    private final boolean predicateUsable;        // EQ / IN literal(s) on `column` were found
    private int manifestKeyType = 0;              // zero denotes a retained v1 manifest
    private int manifestFieldId = -1;

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
                    return disabled(coverage.snapshotId);
                }
            } catch (Exception e) {
                return disabled(coverage.snapshotId);
            }
        }
        return coverage;
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
     */
    public static RapCoverage load(String dir, FileIO io, String uuid, Optional<Long> snapshotId, ScalarOperator predicate) {
        if (dir == null || dir.isEmpty() || io == null || snapshotId == null || !snapshotId.isPresent()) {
            return disabled(snapshotId != null && snapshotId.isPresent() ? snapshotId.get() : -1L);
        }
        long snap = snapshotId.get();
        try {
            if (uuid == null || uuid.isEmpty()) {
                return disabled(snap);
            }
            String path = dir + (dir.endsWith("/") ? "" : "/") + uuid + "/" + snap + SUFFIX;
            InputFile in = io.newInputFile(path);
            if (!in.exists()) {
                LOG.info("RAP manifest absent for {} snapshot {} ({}) -- ordinary scan", uuid, snap, path);
                return disabled(snap);
            }
            // slice 4 (PRD-02): the byte ceiling is applied BEFORE the read; an oversize manifest is "off", never a
            // partial parse
            long length = in.getLength();
            if (length > Config.rap_manifest_max_bytes) {
                LOG.warn("RAP manifest for {} snapshot {} is {} bytes, above rap_manifest_max_bytes {} -- ordinary scan",
                        uuid, snap, length, Config.rap_manifest_max_bytes);
                return disabled(snap);
            }
            String json;
            try (InputStream s = in.newStream()) {
                json = new String(s.readAllBytes(), StandardCharsets.UTF_8);
            }
            return fromJson(json, uuid, snap, predicate);
        } catch (Exception e) {
            LOG.warn("RAP manifest unusable for snapshot {}: {} -- ordinary scan", snap, e.toString());
            return disabled(snap);
        }
    }

    /**
     * Parse a manifest and bind it to the expected (table uuid, snapshot) and the predicate. A manifest
     * for another table or snapshot, or an unknown version, yields a disabled coverage.
     */
    public static RapCoverage fromJson(String json, String expectUuid, long expectSnapshot, ScalarOperator predicate) {
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
                return disabled(expectSnapshot);
            }
            if (!m.has("snapshot_id") || m.get("snapshot_id").getAsLong() != expectSnapshot) {
                LOG.warn("RAP manifest is for snapshot {} not {} -- ordinary scan",
                        m.has("snapshot_id") ? m.get("snapshot_id").getAsString() : "?", expectSnapshot);
                return disabled(expectSnapshot);
            }
            // astra CX-29: the table identity is REQUIRED, not optional -- a manifest that does not declare
            // its table, or declares another one, never activates
            if (!m.has("table_uuid") || m.get("table_uuid").isJsonNull() || m.get("table_uuid").getAsString().isEmpty()) {
                LOG.warn("RAP manifest declares no table_uuid -- ordinary scan");
                return disabled(expectSnapshot);
            }
            if (expectUuid == null || !expectUuid.equals(m.get("table_uuid").getAsString())) {
                LOG.warn("RAP manifest is for table {} not {} -- ordinary scan", m.get("table_uuid").getAsString(), expectUuid);
                return disabled(expectSnapshot);
            }
            String column = m.get("column").getAsString();
            Set<Integer> typedCandidates = version == TYPED_VERSION ? typedCandidates(m, predicate, column) : null;
            List<String> literals = version == VERSION ? extractLiterals(predicate, column) : null;
            boolean hasPostings = m.has("postings") && m.get("postings").isJsonObject();
            RapCoverage c = new RapCoverage(true, expectSnapshot, column, hasPostings,
                    version == TYPED_VERSION ? typedCandidates != null : literals != null);
            if (version == TYPED_VERSION) {
                c.manifestKeyType = m.get("key_type").getAsBigDecimal().intValueExact();
                c.manifestFieldId = m.get("field_id").getAsBigDecimal().intValueExact();
                if (c.manifestFieldId <= 0) {
                    return disabled(expectSnapshot);
                }
            }
            JsonArray files = m.getAsJsonArray("files");
            List<String> names = new ArrayList<>();
            for (JsonElement e : files) {
                JsonObject f = e.getAsJsonObject();
                String name = f.get("name").getAsString();
                if (c.files.containsKey(name)) {
                    return disabled(expectSnapshot);
                }
                names.add(name);
                c.files.put(name, new Covered(f.get("size").getAsLong(), f.get("rows").getAsLong()));
            }
            if (hasPostings) {
                // astra CX-29: EVERY posting is validated before the manifest may eliminate anything -- an
                // index outside `files`, a non-integer, a non-array or an empty list is a malformed manifest,
                // and a malformed manifest keeps every file (never "ignore the bad entry and drop the rest")
                JsonObject postings = m.getAsJsonObject("postings");
                for (Map.Entry<String, JsonElement> e : postings.entrySet()) {
                    if (!e.getValue().isJsonArray() || e.getValue().getAsJsonArray().size() == 0) {
                        LOG.warn("RAP manifest posting for a value is not a non-empty array -- ordinary scan");
                        return disabled(expectSnapshot);
                    }
                    for (JsonElement idx : e.getValue().getAsJsonArray()) {
                        // astra CX-29 (second round): Gson's getAsInt() NARROWS -- 0.5, -0.5 and 4294967296 all
                        // become 0 and would pass a post-narrowing bounds check. Integrality and range are decided
                        // on the exact decimal value BEFORE any narrowing.
                        if (!idx.isJsonPrimitive() || !idx.getAsJsonPrimitive().isNumber()) {
                            LOG.warn("RAP manifest posting index is not a number -- ordinary scan");
                            return disabled(expectSnapshot);
                        }
                        java.math.BigDecimal exact;
                        try {
                            exact = idx.getAsJsonPrimitive().getAsBigDecimal();
                        } catch (NumberFormatException nfe) {
                            LOG.warn("RAP manifest posting index is not a decimal number -- ordinary scan");
                            return disabled(expectSnapshot);
                        }
                        if (exact.stripTrailingZeros().scale() > 0) {
                            LOG.warn("RAP manifest posting index {} is not an integer -- ordinary scan", exact);
                            return disabled(expectSnapshot);
                        }
                        if (exact.signum() < 0 || exact.compareTo(java.math.BigDecimal.valueOf(names.size() - 1)) > 0) {
                            LOG.warn("RAP manifest posting index {} outside its {} files -- ordinary scan", exact, names.size());
                            return disabled(expectSnapshot);
                        }
                    }
                }
                if (literals != null) {
                    for (String lit : literals) {
                        if (!postings.has(lit)) {
                            continue;
                        }
                        for (JsonElement idx : postings.getAsJsonArray(lit)) {
                            // every index was proven an exact integer within [0, files) above
                            c.matching.add(names.get(idx.getAsJsonPrimitive().getAsBigDecimal().intValueExact()));
                        }
                    }
                }
                if (typedCandidates != null) {
                    for (int index : typedCandidates) {
                        c.matching.add(names.get(index));
                    }
                }
            }
            return c;
        } catch (Exception e) {
            LOG.warn("RAP manifest malformed ({}) -- ordinary scan", e.getClass().getSimpleName());
            return disabled(expectSnapshot);
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

    private static Set<Integer> typedCandidates(JsonObject manifest, ScalarOperator predicate, String column) {
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
        Set<Integer> nullFiles = fileIndices(manifest.get("null_postings"), size, true);
        return RapManifestPredicate.matching(predicate, column, type, postings, nullFiles);
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

    public String explain() {
        return String.format("RAP MANIFEST: %s snapshot=%d column=%s postings=%s consulted=%d covered=%d "
                        + "identity_mismatch=%d dropped=%d",
                active ? "active" : "off", snapshotId, column, hasPostings ? "yes" : "no",
                consulted, covered, identityMismatch, dropped);
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
