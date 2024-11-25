/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.cql.statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;

public class OriginSelectStatementTest extends CommonMocks {

    TestOriginSelectStatement originSelectStatement;

    String bindClause = "1=1";

    @BeforeEach
    public void setup() {
        commonSetup();
        originSelectStatement = new TestOriginSelectStatement(propertyHelper, originSession);
    }

    @Test
    public void smoke_basicCQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(String.join(",", originColumnNames)).append(" FROM ")
                .append(originKeyspaceTableName).append(" WHERE ").append(bindClause);

        String cql = originSelectStatement.getCQL();
        assertEquals(sb.toString(), cql);
    }

    @Test
    public void writetime_filter() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWriteTimestampFilter()).thenReturn(true);
        when(writetimeTTLFeature.getMinWriteTimeStampFilter()).thenReturn(1000L);
        when(writetimeTTLFeature.getMaxWriteTimeStampFilter()).thenReturn(2000L);
        originSelectStatement = new TestOriginSelectStatement(propertyHelper, originSession);

        assertAll(
                () -> {
                    when(writetimeTTLFeature.getLargestWriteTimeStamp(record.getOriginRow())).thenReturn(1500L);
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "timestamp is range");
                },
                () -> {
                    when(writetimeTTLFeature.getLargestWriteTimeStamp(record.getOriginRow())).thenReturn(500L);
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "timestamp below range");
                },
                () -> {
                    when(writetimeTTLFeature.getLargestWriteTimeStamp(record.getOriginRow())).thenReturn(2500L);
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "timestamp above range");
                },
                () -> {
                    when(writetimeTTLFeature.getLargestWriteTimeStamp(record.getOriginRow())).thenReturn(null);
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "null timestamp");
                }
        );
    }

    @Test
    public void column_filter_values() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWriteTimestampFilter()).thenReturn(false);
        when(propertyHelper.getAsString(KnownProperties.FILTER_COLUMN_NAME)).thenReturn(filterCol);
        when(propertyHelper.getString(KnownProperties.FILTER_COLUMN_VALUE)).thenReturn("abc");

        originSelectStatement = new TestOriginSelectStatement(propertyHelper, originSession);

        assertAll(
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("abc");
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "match string");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("  abc  ");
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "match string with whitespace");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("ABC");
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "match string but uppercase");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("abc123");
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "mismatch string");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("");
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "empty string");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn(null);
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "null string");
                }
        );
    }

    @Test
    public void isRecordValid() {
        assertAll(() -> {
            assertFalse(originSelectStatement.isRecordValid(null), "null record");
        }, () -> {
            assertTrue(originSelectStatement.isRecordValid(record), "valid row");
        }, () -> {
            when(record.getPk().isError()).thenReturn(true);
            when(record.getPk().isWarning()).thenReturn(false);
            assertFalse(originSelectStatement.isRecordValid(record), "error PK");
        }, () -> {
            when(record.getPk().isError()).thenReturn(false);
            when(record.getPk().isWarning()).thenReturn(true);
            assertTrue(originSelectStatement.isRecordValid(record), "warning PK");
        });
    }

    @Test
    public void column_filter_record() {
        when(writetimeTTLFeature.isEnabled()).thenReturn(true);
        when(writetimeTTLFeature.hasWriteTimestampFilter()).thenReturn(false);
        when(propertyHelper.getAsString(KnownProperties.FILTER_COLUMN_NAME)).thenReturn(filterCol);
        when(propertyHelper.getString(KnownProperties.FILTER_COLUMN_VALUE)).thenReturn("abc");
        when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("do not filter me");
        when(record.getOriginRow()).thenReturn(originRow);

        originSelectStatement = new TestOriginSelectStatement(propertyHelper, originSession);

        assertAll(
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("abc");
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "match string");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("  abc  ");
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "match string with whitespace");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("ABC");
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "match string but uppercase");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("abc123");
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "mismatch string");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn("");
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "empty string");
                },
                () -> {
                    when(originTable.getData(originColumnNames.indexOf(filterCol), record.getOriginRow())).thenReturn(null);
                    assertFalse(originSelectStatement.shouldFilterRecord(record), "null string");
                },
                () -> {
                    assertTrue(originSelectStatement.shouldFilterRecord(null), "null record");
                },
                () -> {
                    when(record.getPk().isError()).thenReturn(true);
                    assertTrue(originSelectStatement.shouldFilterRecord(record), "error PK");
                }
        );
    }

    @Test
    public void testExecute() {
        ResultSet result = originSelectStatement.execute(boundStatement);
        assertEquals(originResultSet, result);
    }

    @Test
    public void test_emptyColumns() {
        when(originTable.getColumnNames(false)).thenReturn(Collections.emptyList());
        assertThrows(RuntimeException.class, () -> new TestOriginSelectStatement(propertyHelper, originSession));
    }

    protected class TestOriginSelectStatement extends OriginSelectStatement {
        public TestOriginSelectStatement(IPropertyHelper h, EnhancedSession s) {
            super(h, s);
        }

        @Override
        public BoundStatement bind(Object... binds) {
            return boundStatement;
        };

        @Override
        protected String whereBinds() {
            return bindClause;
        };
    }

}
