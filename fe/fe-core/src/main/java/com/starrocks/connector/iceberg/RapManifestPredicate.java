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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/** Typed file candidates for RAP manifest v2. Null means unsupported, NOT an empty candidate set. */
final class RapManifestPredicate {
    private RapManifestPredicate() {
    }

    static int keyType(Type type) {
        switch (type.getPrimitiveType()) {
            case VARCHAR:
                return 1;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return 2;
            case BOOLEAN:
                return 3;
            case DATE:
                return 4;
            case DATETIME:
                return 5;
            default:
                // CHAR padding/collation, floating point, decimals and casts need their own contract.
                return 0;
        }
    }

    static String encodeLong(long value) {
        return HexFormat.of().formatHex(ByteBuffer.allocate(Long.BYTES).putLong(value ^ Long.MIN_VALUE).array());
    }

    /** Matches RapIndex::encode_literal, DateValue::julian and timestamp::from_julian_and_time. */
    static String literal(ConstantOperator value, int keyType) {
        if (value.isNull() || keyType(value.getType()) != keyType) {
            return null;
        }
        switch (value.getType().getPrimitiveType()) {
            case VARCHAR:
                return HexFormat.of().formatHex(value.getVarchar().getBytes(StandardCharsets.UTF_8));
            case TINYINT:
                return encodeLong(value.getTinyInt());
            case SMALLINT:
                return encodeLong(value.getSmallint());
            case INT:
                return encodeLong(value.getInt());
            case BIGINT:
                return encodeLong(value.getBigint());
            case BOOLEAN:
                return value.getBoolean() ? "01" : "00";
            case DATE:
                return encodeLong(value.getDate().toLocalDate().toEpochDay() + 2440588L);
            case DATETIME:
                LocalDateTime date = value.getDatetime();
                // Do not silently truncate unsupported sub-microsecond literals.
                if (date.getNano() % 1000 != 0 || date.getYear() < 0 || date.getYear() > 9999) {
                    return null;
                }
                long julian = date.toLocalDate().toEpochDay() + 2440588L;
                return encodeLong((julian << 40) | (date.toLocalTime().toNanoOfDay() / 1000));
            default:
                return null;
        }
    }

    static void validateKey(String key, int type) {
        if (type < 1 || type > 5 || !key.matches("(?:[0-9a-f]{2})*")) {
            throw new IllegalArgumentException("invalid RAP key encoding/type");
        }
        if ((type == 2 || type == 4 || type == 5) && key.length() != 16) {
            throw new IllegalArgumentException("invalid RAP fixed-width key");
        }
        if (type == 3 && !key.equals("00") && !key.equals("01")) {
            throw new IllegalArgumentException("invalid RAP boolean key");
        }
    }

    private static boolean column(ScalarOperator op, String column, int type) {
        return op instanceof ColumnRefOperator && column.equalsIgnoreCase(((ColumnRefOperator) op).getName())
                && keyType(op.getType()) == type;
    }

    private static Set<Integer> union(Map<String, Set<Integer>> postings) {
        Set<Integer> out = new HashSet<>();
        postings.values().forEach(out::addAll);
        return out;
    }

    static Set<Integer> matching(ScalarOperator op, String column, int type,
                                 NavigableMap<String, Set<Integer>> postings, Set<Integer> nullFiles) {
        if (op == null) {
            return null;
        }
        if (op instanceof CompoundPredicateOperator) {
            if (!((CompoundPredicateOperator) op).isAnd()) {
                return null; // Never narrow using just one branch of OR/NOT.
            }
            Set<Integer> out = null;
            for (ScalarOperator child : op.getChildren()) {
                Set<Integer> next = matching(child, column, type, postings, nullFiles);
                if (next != null) {
                    if (out == null) {
                        out = new HashSet<>(next);
                    } else {
                        out.retainAll(next);
                    }
                }
            }
            return out;
        }
        if (op instanceof IsNullPredicateOperator && column(op.getChild(0), column, type)) {
            return ((IsNullPredicateOperator) op).isNotNull() ? union(postings) : new HashSet<>(nullFiles);
        }
        if (op instanceof InPredicateOperator) {
            InPredicateOperator in = (InPredicateOperator) op;
            if (in.isNotIn() || !column(in.getChild(0), column, type)) {
                return null;
            }
            Set<Integer> out = new HashSet<>();
            for (int i = 1; i < in.getChildren().size(); i++) {
                if (!(in.getChild(i) instanceof ConstantOperator)) {
                    return null; // Discard partial results if ANY member is unsupported.
                }
                String key = literal((ConstantOperator) in.getChild(i), type);
                if (key == null) {
                    return null;
                }
                out.addAll(postings.getOrDefault(key, Set.of()));
            }
            return out;
        }
        if (!(op instanceof BinaryPredicateOperator)) {
            return null;
        }
        BinaryPredicateOperator binary = (BinaryPredicateOperator) op;
        ScalarOperator left = op.getChild(0);
        ScalarOperator right = op.getChild(1);
        boolean reverse = false;
        if (!column(left, column, type)) {
            ScalarOperator swap = left;
            left = right;
            right = swap;
            reverse = true;
        }
        if (!column(left, column, type) || !(right instanceof ConstantOperator)) {
            return null;
        }
        String key = literal((ConstantOperator) right, type);
        if (key == null) {
            return null;
        }
        BinaryType kind = binary.getBinaryType();
        switch (kind) {
            case EQ:
                return new HashSet<>(postings.getOrDefault(key, Set.of()));
            case GT:
            case GE:
                return union(reverse ? postings.headMap(key, kind == BinaryType.GE)
                        : postings.tailMap(key, kind == BinaryType.GE));
            case LT:
            case LE:
                return union(reverse ? postings.tailMap(key, kind == BinaryType.LE)
                        : postings.headMap(key, kind == BinaryType.LE));
            default:
                return null;
        }
    }
}
