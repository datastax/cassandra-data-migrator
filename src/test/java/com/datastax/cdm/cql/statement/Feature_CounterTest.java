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

import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class Feature_CounterTest {

    @Mock
    IPropertyHelper propertyHelper;

    @Mock
    CqlTable originTable;

    @Mock
    CqlTable targetTable;

    List<String> standardNames = Arrays.asList("key", "col1", "col2");
    List<DataType> standardDataTypes = Arrays.asList(DataTypes.TIMESTAMP, DataTypes.COUNTER, DataTypes.COUNTER);
    List<Class> standardBindClasses = Arrays.asList(CqlData.getBindClass(standardDataTypes.get(0)),
            CqlData.getBindClass(standardDataTypes.get(1)), CqlData.getBindClass(standardDataTypes.get(2)));

    @BeforeEach
    public void setup() {
        propertyHelper = mock(IPropertyHelper.class);
        originTable = mock(CqlTable.class);
        targetTable = mock(CqlTable.class);
    }

    private void setValidSparkConf() {
    }

    // @Test
    // public void smokeTest_initialize() {
    // setValidSparkConf();
    // helper.initializeSparkConf(validSparkConf);
    // assertAll(
    // () -> assertEquals(Arrays.asList(1,2), helper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEXES),
    // "ORIGIN_COUNTER_INDEXES")
    // );
    // }
    //
    // @Test
    // public void smokeCQL() {
    // SparkConf sparkConf = new SparkConf();
    // sparkConf.set(KnownProperties.ORIGIN_CONNECT_HOST, "localhost");
    // sparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "origin.tab1");
    // sparkConf.set(KnownProperties.ORIGIN_COLUMN_NAMES, "key,col1,col2");
    // sparkConf.set(KnownProperties.ORIGIN_COLUMN_TYPES, "4,2,2");
    // sparkConf.set(KnownProperties.ORIGIN_PARTITION_KEY, "key");
    // sparkConf.set(KnownProperties.ORIGIN_COUNTER_INDEXES, "1,2");
    //
    // sparkConf.set(KnownProperties.TARGET_PRIMARY_KEY, "key");
    // sparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "target.tab1");
    //
    // helper.initializeSparkConf(sparkConf);
    // CqlHelper cqlHelper = new CqlHelper();
    // cqlHelper.initialize();
    //
    // String originSelect = "SELECT key,col1,col2 FROM origin.tab1 WHERE TOKEN(key) >= ? AND TOKEN(key) <= ? ALLOW
    // FILTERING";
    // String originSelectByPK = "SELECT key,col1,col2 FROM origin.tab1 WHERE key=?";
    // String targetUpdate = "UPDATE target.tab1 SET col1=col1+?,col2=col2+? WHERE key=?";
    // String targetSelect = "SELECT key,col1,col2 FROM target.tab1 WHERE key=?";
    //
    // assertAll(
    // () -> assertEquals(originSelect,
    // cqlHelper.getOriginSelectByPartitionRangeStatement(null).getCQL().replaceAll("\\s+"," ")),
    // () -> assertEquals(originSelectByPK, cqlHelper.getOriginSelectByPKStatement(null).getCQL().replaceAll("\\s+","
    // ")),
    // () -> assertEquals(targetUpdate, cqlHelper.getTargetUpdateStatement(null).getCQL().replaceAll("\\s+"," ")),
    // () -> assertEquals(targetSelect, cqlHelper.getTargetSelectByPKStatement(null).getCQL().replaceAll("\\s+"," "))
    // );
    // }
}
