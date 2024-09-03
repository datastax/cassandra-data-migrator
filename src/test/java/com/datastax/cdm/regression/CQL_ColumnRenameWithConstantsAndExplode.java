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
package com.datastax.cdm.regression;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.cql.statement.*;
import com.datastax.oss.driver.api.core.type.DataTypes;

/*
   This test addresses CDM-34 which needs to migrate from:
      CREATE TABLE "CUSTOMER"."Index" (
          "parameter-name" text PRIMARY KEY,
          "parameter-value" map<text, text>)

   to:
      CREATE TABLE astra.indextable (
          customer text,
          parameter_name text,
          id text,
          value text,
          PRIMARY KEY ((customer, parameter_name), id))

   1. Table to be renamed from Index (mixed case) to lowercase indextable
   2. customer column to be hard-coded to value 'CUSTOMER' (corresponds with keyspace name)
   3. "parameter-name" column to be renamed to lowercase parameter_name with underscore
   4. "parameter-value" map to be exploded into two columns (and multiple rows): id and value

 */
public class CQL_ColumnRenameWithConstantsAndExplode extends CommonMocks {

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        setTestVariables();
        commonSetupWithoutDefaultClassVariables(true, true, false);
    }

    // Set up table as it would be in Cassandra
    private void setTestVariables() {
        originKeyspaceName = "\"CUSTOMER\"";
        originTableName = "\"Index\"";
        originPartitionKey = Collections.singletonList("parameter-name");
        originPartitionKeyTypes = Collections.singletonList(DataTypes.TEXT);
        originClusteringKey = Collections.emptyList();
        originClusteringKeyTypes = Collections.emptyList();
        originValueColumns = Arrays.asList("parameter-value");
        originValueColumnTypes = Arrays.asList(DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT));

        targetKeyspaceName = "astra";
        targetTableName = "indextable";
        targetPartitionKey = Arrays.asList("customer", "parameter_name");
        targetPartitionKeyTypes = Arrays.asList(DataTypes.TEXT, DataTypes.TEXT);
        targetClusteringKey = Collections.singletonList("id");
        targetClusteringKeyTypes = Collections.singletonList(DataTypes.TEXT);
        targetValueColumns = Arrays.asList("value");
        targetValueColumnTypes = Arrays.asList(DataTypes.TEXT);

        constantColumns = Collections.singletonList("customer");
        constantColumnTypes = Collections.singletonList(DataTypes.TEXT);
        constantColumnValues = Collections.singletonList("'CUSTOMER'");

        explodeMapColumn = "parameter-value";
        explodeMapKey = "id";
        explodeMapKeyType = DataTypes.TEXT;
        explodeMapValue = "value";
        explodeMapValueType = DataTypes.TEXT;
    }

    @Test
    public void smokeCQL() {
        String originSelectString = "SELECT \"parameter-name\",\"parameter-value\" FROM \"CUSTOMER\".\"Index\" WHERE TOKEN(\"parameter-name\") >= ? AND TOKEN(\"parameter-name\") <= ? ALLOW FILTERING";
        String originSelectByPKString = "SELECT \"parameter-name\",\"parameter-value\" FROM \"CUSTOMER\".\"Index\" WHERE \"parameter-name\"=?";
        String targetInsertString = "INSERT INTO astra.indextable (parameter_name,id,value,customer) VALUES (?,?,?,'CUSTOMER')";
        String targetUpdateString = "UPDATE astra.indextable SET value=? WHERE customer='CUSTOMER' AND parameter_name=? AND id=?";
        String targetSelectString = "SELECT customer,parameter_name,id,value FROM astra.indextable WHERE customer='CUSTOMER' AND parameter_name=? AND id=?";

        OriginSelectByPartitionRangeStatement originSelect = new OriginSelectByPartitionRangeStatement(propertyHelper,
                originSession);
        OriginSelectByPKStatement originSelectByPK = new OriginSelectByPKStatement(propertyHelper, originSession);
        TargetInsertStatement targetInsert = new TargetInsertStatement(propertyHelper, targetSession);
        TargetUpdateStatement targetUpdate = new TargetUpdateStatement(propertyHelper, targetSession);
        TargetSelectByPKStatement targetSelect = new TargetSelectByPKStatement(propertyHelper, targetSession);

        assertAll(() -> assertEquals(originSelectString, originSelect.getCQL().replaceAll("\\s+", " "), "originSelect"),
                () -> assertEquals(originSelectByPKString, originSelectByPK.getCQL().replaceAll("\\s+", " "),
                        "originSelectByPK"),
                () -> assertEquals(targetInsertString, targetInsert.getCQL().replaceAll("\\s+", " "), "targetInsert"),
                () -> assertEquals(targetUpdateString, targetUpdate.getCQL().replaceAll("\\s+", " "), "targetUpdate"),
                () -> assertEquals(targetSelectString, targetSelect.getCQL().replaceAll("\\s+", " "), "targetSelect"));
    }

}
