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
package com.datastax.cdm.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TokenOverlapTest {

    @Mock
    private CqlSession cqlSession;

    @Mock
    private Metadata metadata;

    // Cannot mock Optional directly as it's a final class

    @Mock
    private TokenMap tokenMap;

    @Mock
    private KeyspaceMetadata keyspaceMetadata;

    @Mock
    private TableMetadata tableMetadata;

    @Mock
    private IPropertyHelper propertyHelper;

    @Mock
    private ResultSet resultSet;

    @Mock
    private Row row1, row2, row3;

    private CqlTable cqlTable;
    private final String keyspaceName = "test_keyspace";
    private final String tableName = "test_table";

    @BeforeEach
    void setUp() {
        // Setup property helper - with specific mocking for ORIGIN_KEYSPACE_TABLE first
        // This is required for the BaseTable constructor to work
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE))
                .thenReturn(keyspaceName + "." + tableName);

        // Use lenient mocking for the rest to avoid PotentialStubbingProblem
        lenient().when(propertyHelper.getBoolean(KnownProperties.EXTRACT_JSON_EXCLUSIVE)).thenReturn(false);
        lenient().when(propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES_TO_SKIP))
                .thenReturn(Collections.emptyList());
        lenient().when(propertyHelper.getBoolean(KnownProperties.ALLOW_COLL_FOR_WRITETIME_TTL_CALC)).thenReturn(false);
        lenient().when(propertyHelper.getString(KnownProperties.READ_CL)).thenReturn("LOCAL_QUORUM");
        lenient().when(propertyHelper.getString(KnownProperties.WRITE_CL)).thenReturn("LOCAL_QUORUM");
        lenient().when(propertyHelper.getLong(KnownProperties.TRANSFORM_REPLACE_MISSING_TS)).thenReturn(null);

        // Mock all other possible property helper methods that might be called
        lenient().when(propertyHelper.getBoolean(anyString())).thenReturn(false);
        lenient().when(propertyHelper.getInteger(anyString())).thenReturn(1000);
        lenient().when(propertyHelper.getNumber(anyString())).thenReturn(1000);
        lenient().when(propertyHelper.getAsString(anyString())).thenReturn("");
        // Don't use lenient for getString with anyString() as it would conflict with the specific ORIGIN_KEYSPACE_TABLE mock
        lenient().when(propertyHelper.getStringList(anyString())).thenReturn(new ArrayList<>());
    }

    @Test
    void testTokenMapNotPresent() {
        // Setup metadata mock to return empty TokenMap optional
        lenient().when(cqlSession.getMetadata()).thenReturn(metadata);
        lenient().when(metadata.getTokenMap()).thenReturn(Optional.empty());

        // Setup keyspace and table metadata to get past the initial checks
        lenient().when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(keyspaceMetadata));
        lenient().when(keyspaceMetadata.getTable(any(CqlIdentifier.class))).thenReturn(Optional.of(tableMetadata));

        // Create cqlTable with mocked session should throw exception
        ClusterConfigurationException exception = assertThrows(ClusterConfigurationException.class,
                () -> new CqlTable(propertyHelper, true, cqlSession));

        // Verify exception message
        assertEquals("Token map is not available. This could indicate a cluster configuration issue.",
                exception.getMessage());
    }

    @Test
    void testTokenOverlapDetected() {
        // Setup metadata
        when(cqlSession.getMetadata()).thenReturn(metadata);
        when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
        when(tokenMap.getPartitionerName()).thenReturn("Murmur3Partitioner");

        // Setup keyspace and table metadata
        CqlIdentifier keyspaceId = CqlIdentifier.fromCql(keyspaceName);
        when(metadata.getKeyspace(keyspaceId)).thenReturn(Optional.of(keyspaceMetadata));
        CqlIdentifier tableId = CqlIdentifier.fromCql(tableName);
        when(keyspaceMetadata.getTable(tableId)).thenReturn(Optional.of(tableMetadata));

        // Mock columns and primary keys
        when(tableMetadata.getPartitionKey()).thenReturn(new ArrayList<>());
        when(tableMetadata.getClusteringColumns()).thenReturn(Collections.emptyMap());
        when(tableMetadata.getColumns()).thenReturn(Collections.emptyMap());

        // Mock token overlap detection query
        when(cqlSession.execute(anyString(), eq(keyspaceName))).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(Arrays.asList(row1, row2, row3).iterator());

        // Setup rows to indicate overlap (range 1: 0-100, range 2: 50-150, range 3: 200-300)
        when(row1.getString("start_token")).thenReturn("0");
        when(row1.getString("end_token")).thenReturn("100");
        when(row2.getString("start_token")).thenReturn("50"); // Overlaps with first range
        when(row2.getString("end_token")).thenReturn("150");
        when(row3.getString("start_token")).thenReturn("200");
        when(row3.getString("end_token")).thenReturn("300");

        // Creating cqlTable should throw exception due to token overlap
        ClusterConfigurationException exception = assertThrows(
            ClusterConfigurationException.class,
            () -> new CqlTable(propertyHelper, true, cqlSession)
        );

        // Verify exception message contains keyspace name and token overlap info
        String expectedMessage = "Token overlap detected in keyspace 'test_keyspace'. This usually happens when multiple nodes were started simultaneously";
        assertEquals(true, exception.getMessage().contains(expectedMessage));
    }

    @Test
    void testNoTokenOverlap() {
        // This test verifies that non-overlapping token ranges don't cause the ClusterConfigurationException
        // We'll simulate just enough to test the token overlap detection logic

        // Setup minimal metadata needed to reach the token overlap check
        when(cqlSession.getMetadata()).thenReturn(metadata);
        when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
        when(tokenMap.getPartitionerName()).thenReturn("Murmur3Partitioner");

        // Setup keyspace and table metadata
        when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(keyspaceMetadata));
        when(keyspaceMetadata.getTable(any(CqlIdentifier.class))).thenReturn(Optional.of(tableMetadata));

        // Mock token query with non-overlapping ranges
        when(cqlSession.execute(anyString(), eq(keyspaceName))).thenReturn(resultSet);

        // Create a simple iterator with just one row - no overlap possible
        Row singleRow = mock(Row.class);
        when(singleRow.getString("start_token")).thenReturn("0");
        when(singleRow.getString("end_token")).thenReturn("100");
        when(resultSet.iterator()).thenReturn(Collections.singletonList(singleRow).iterator());

        // No need to force an exception since the test will encounter a real error
        // after the token overlap check passes - with our mocking, we should get a "Table not found" error

        // Should throw RuntimeException due to a table not being found, not ClusterConfigurationException
        Exception exception = assertThrows(RuntimeException.class,
            () -> new CqlTable(propertyHelper, true, cqlSession));

        // Verify it contains the expected message
        assertEquals("Table not found: test_table", exception.getMessage());
    }
}
