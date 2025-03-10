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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.oss.driver.api.core.ConsistencyLevel;

class CqlTableTest extends CommonMocks {

    @Test
    void testCL() {
        assertEquals(CqlTable.mapToConsistencyLevel("LOCAL_QUORUM"), ConsistencyLevel.LOCAL_QUORUM);
        assertEquals(CqlTable.mapToConsistencyLevel("any"), ConsistencyLevel.ANY);
        assertEquals(CqlTable.mapToConsistencyLevel("one"), ConsistencyLevel.ONE);
        assertEquals(CqlTable.mapToConsistencyLevel("two"), ConsistencyLevel.TWO);
        assertEquals(CqlTable.mapToConsistencyLevel("three"), ConsistencyLevel.THREE);
        assertEquals(CqlTable.mapToConsistencyLevel("QUORUM"), ConsistencyLevel.QUORUM);
        assertEquals(CqlTable.mapToConsistencyLevel("Local_one"), ConsistencyLevel.LOCAL_ONE);
        assertEquals(CqlTable.mapToConsistencyLevel("EACH_quorum"), ConsistencyLevel.EACH_QUORUM);
        assertEquals(CqlTable.mapToConsistencyLevel("serial"), ConsistencyLevel.SERIAL);
        assertEquals(CqlTable.mapToConsistencyLevel("local_serial"), ConsistencyLevel.LOCAL_SERIAL);
        assertEquals(CqlTable.mapToConsistencyLevel("all"), ConsistencyLevel.ALL);
    }

    @Test
    void testformatName() {
        assertNull(CqlTable.formatName(null));
        assertEquals("", CqlTable.formatName(""));
        assertEquals("\"KS123ks.T123able\"", CqlTable.formatName("KS123ks.T123able"));
        assertEquals("\"Ks.Table\"", CqlTable.formatName("\"Ks.Table\""));
        assertEquals("\"ks.table\"", CqlTable.formatName("ks.table"));
    }
}
