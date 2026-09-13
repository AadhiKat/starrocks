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

package com.starrocks.qe;

import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RapBuildTokenTest {
    @Test
    public void testSessionTransportAndIsolation() throws Exception {
        SessionVariable session = new SessionVariable();
        Assertions.assertFalse(session.toThrift().isSetRap_build_token());
        String token = "rap_build_0123456789abcdef01234567";
        SystemVariable variable = new SystemVariable(SetType.SESSION, "rap_build_token", new StringLiteral(token));
        variable.setResolvedExpression(new StringLiteral(token));
        new VariableMgr().setSystemVariable(session, variable, true);
        Assertions.assertEquals(token, session.toThrift().getRap_build_token());
        Assertions.assertFalse(new SessionVariable().toThrift().isSetRap_build_token());
        SystemVariable global = new SystemVariable(SetType.GLOBAL, "rap_build_token", new StringLiteral(token));
        global.setResolvedExpression(new StringLiteral(token));
        Assertions.assertThrows(com.starrocks.common.DdlException.class,
                () -> new VariableMgr().setSystemVariable(session, global, false));
    }

    @Test
    public void testTaskHintSyntax() throws Exception {
        String sql = "SUBMIT TASK poc.rap_build_0123456789abcdef01234567 AS " +
                "INSERT /*+ SET_VAR(rap_build_token='rap_build_0123456789abcdef01234567') */ " +
                "INTO blackhole() SELECT v FROM ice_poc.poc_lake.fixture FOR VERSION AS OF 10";
        SubmitTaskStmt task = (SubmitTaskStmt) SqlParser.parse(sql, new SessionVariable()).get(0);
        task.setOrigStmt(new OriginStatement(sql, 0));
        // Catalog resolution is outside this transport test. Exercise the real task SQL
        // extraction and reparsing while supplying its already-checked temporary-table predicate.
        new MockUp<AnalyzerUtils>() {
            @Mock
            public boolean hasTemporaryTables(StatementBase statement) {
                return false;
            }
        };
        Analyzer.AnalyzerVisitor.analyzeSubmitTaskOnly(task.getInsertStmt(), task, new ConnectContext());
        Assertions.assertTrue(task.getSqlText().contains("rap_build_token='rap_build_0123456789abcdef01234567'"));
        StatementBase execution = SqlParser.parse(task.getSqlText(), new SessionVariable()).get(0);
        Assertions.assertEquals(1, execution.getAllQueryScopeHints().size());
        SessionVariable executionSession = new SessionVariable();
        new VariableMgr().applySessionVariable(execution.getAllQueryScopeHints().get(0).getValue(), executionSession);
        Assertions.assertEquals("rap_build_0123456789abcdef01234567", executionSession.toThrift().getRap_build_token());
    }
}
