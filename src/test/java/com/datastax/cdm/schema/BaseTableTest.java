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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.properties.KnownProperties;

public class BaseTableTest extends CommonMocks {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        commonSetupWithoutDefaultClassVariables();
    }

    @Test
    public void useOriginWhenTargetAbsent() {
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE)).thenReturn("origin_ks.origin_table");
        BaseTable bt = new BaseTable(propertyHelper, false);

        assertAll(
                () -> assertEquals(false, bt.isOrigin()),
                () -> assertEquals("origin_ks", bt.getKeyspaceName()),
                () -> assertEquals("origin_table", bt.getTableName()),
                () -> assertEquals("origin_ks.origin_table", bt.getKeyspaceTable())
        );
    }

    @Test
    public void useKSAbsent() {
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE)).thenReturn("origin_table");
        BaseTable bt = new BaseTable(propertyHelper, false);

        assertAll(
                () -> assertEquals("", bt.getKeyspaceName()),
                () -> assertEquals("origin_table", bt.getTableName())
        );
    }

    @Test
    public void useTargetWhenTargetPresent() {
        when(propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE)).thenReturn("origin_ks.origin_table");
        when(propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE)).thenReturn("target_ks.target_table");
        BaseTable bt = new BaseTable(propertyHelper, false);

        assertAll(
                () -> assertEquals("target_ks", bt.getKeyspaceName()),
                () -> assertEquals("target_table", bt.getTableName())
        );
    }

    @Test
    public void failWhenKsTableAbsent() {
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> new BaseTable(propertyHelper, false));
        assertTrue(thrown.getMessage().contentEquals(
                "Value for required property " + KnownProperties.ORIGIN_KEYSPACE_TABLE + " not provided!!"));
    }
}
