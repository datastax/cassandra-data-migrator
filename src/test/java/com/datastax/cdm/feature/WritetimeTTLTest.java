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
package com.datastax.cdm.feature;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class WritetimeTTLTest extends CommonMocks {

    WritetimeTTL feature;
    Long customWritetime = 123456789L;
    Long customTTL = 1000L;
    Long filterMin = 100000000L;
    Long filterMax = 200000000L;
    String writetimeColumnName = "writetime_col";
    String ttlColumnName = "ttl_col";
    String writetimeTTLColumnName = "writetime_ttl_col";

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        setTestVariables();
        commonSetupWithoutDefaultClassVariables(false, false, false);
        setTestWhens();
        feature = new WritetimeTTL();
    }

    private void setTestVariables() {
        originValueColumns = new ArrayList<>();
        originValueColumns.addAll(Arrays.asList(writetimeColumnName, ttlColumnName, writetimeTTLColumnName));
        originValueColumnTypes = new ArrayList<>(originValueColumnTypes);
        originValueColumnTypes.addAll(Arrays.asList(DataTypes.TEXT, DataTypes.TEXT, DataTypes.TEXT));
    }

    private void setTestWhens(){
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(customWritetime);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(Arrays.asList(writetimeColumnName,writetimeTTLColumnName));
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(Arrays.asList(ttlColumnName,writetimeTTLColumnName));
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(false);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(false);
        when(originTable.getWritetimeTTLColumns()).thenReturn(originValueColumns);
        when(originTable.isWritetimeTTLColumn(anyString())).thenAnswer(invocation -> {
            String argument = invocation.getArgument(0);
            return originValueColumns.contains(argument);
        });
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(customTTL);
    }

    @Test
    public void smoke_loadProperties() {
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(filterMin);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(filterMax);
        feature.loadProperties(propertyHelper);

        assertAll(
                () -> assertTrue(feature.isEnabled(), "enabled"),
                () -> assertEquals(customWritetime, feature.getCustomWritetime(), "customWritetime"),
                () -> assertEquals(customTTL, feature.getCustomTTL(), "customTTL"),
                () -> assertTrue(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns with custom writetime"),
                () -> assertEquals(customWritetime, feature.getCustomWritetime(), "customWritetime is set"),
                () -> assertEquals(filterMin, feature.getMinWriteTimeStampFilter(), "filterMin"),
                () -> assertEquals(filterMax, feature.getMaxWriteTimeStampFilter(), "filterMax")
        );
    }

    @Test
    public void smoke_initializeAndValidate() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(filterMin);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(filterMax);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        assertAll(
                () -> assertEquals(0L, feature.getCustomWritetime(), "customWritetime is not set"),
                () -> assertTrue(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns without custom writetime"),
                () -> assertTrue(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smokeTest_disabledFeature() {
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(Boolean.FALSE);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(Boolean.FALSE);

        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertTrue(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertFalse(feature.isEnabled(), "feature should be disabled"),
                () -> assertEquals(0L, feature.getCustomWritetime(), "customWritetime is not set"),
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertFalse(feature.hasWritetimeColumns(), "hasWritetimeColumns without custom writetime"),
                () -> assertFalse(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smokeTest_enabledFeature_withOnlyWritetimeAuto() {
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(Boolean.TRUE);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(Boolean.FALSE);

        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertTrue(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertTrue(feature.isEnabled(), "feature should be enabled"),
                () -> assertEquals(0L, feature.getCustomWritetime(), "customWritetime is not set"),
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns with Auto set"),
                () -> assertFalse(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smokeTest_enabledFeature_withOnlyTTLAuto() {
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(Boolean.FALSE);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(Boolean.TRUE);

        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertTrue(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertTrue(feature.isEnabled(), "feature should be enabled"),
                () -> assertEquals(0L, feature.getCustomWritetime(), "customWritetime is not set"),
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertFalse(feature.hasWritetimeColumns(), "hasWritetimeColumns"),
                () -> assertTrue(feature.hasTTLColumns(), "hasTTLColumns with ttlAuto set")
        );
    }

    @Test
    public void smoke_writetimeWithoutTTL() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(filterMin);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(filterMax);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertEquals(0L, feature.getCustomWritetime(), "customWritetime is not set"),
                () -> assertTrue(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns without custom writetime"),
                () -> assertFalse(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smoke_ttlWithoutWritetime_noCustomWritetime() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(filterMin);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(filterMax);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertFalse(feature.hasWritetimeColumns(), "hasWritetimeColumns without custom writetime"),
                () -> assertTrue(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smoke_ttlWithoutWritetime_withCustomWritetime() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(100L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertTrue(feature.isEnabled(), "isEnabled"),
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns with custom writetime"),
                () -> assertTrue(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smoke_autoWritetime_noCustomWritetime() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns"),
                () -> assertFalse(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smoke_autoWritetime_CustomWritetime() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(100L);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns"),
                () -> assertFalse(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void smoke_autoTTL() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(true);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertFalse(feature.hasWritetimeColumns(), "hasWritetimeColumns"),
                () -> assertTrue(feature.hasTTLColumns(), "hasTTLColumns")
        );
    }

    @Test
    public void counter_unconfigured() {
        when(originTable.isCounterTable()).thenReturn(true);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(true);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(true);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertFalse(feature.isEnabled(), "isEnabled"),
                () -> assertFalse(feature.isValid, "isValid")
        );
    }

    @Test
    public void counter_configured() {
        when(originTable.isCounterTable()).thenReturn(true);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertFalse(feature.isEnabled(), "isEnabled"),
                () -> assertFalse(feature.isValid, "isValid")
        );
    }

    @Test
    public void test_ttl_noValidColumns() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(true);
        when(originTable.isWritetimeTTLColumn(anyString())).thenReturn(false);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        assertFalse(feature.isEnabled(), "feature should be disabled");
    }

    @Test
    public void test_writetime_noValidColumns() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(originTable.isWritetimeTTLColumn(anyString())).thenReturn(false);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        assertFalse(feature.isEnabled(), "feature should be disabled");
    }

    @Test
    public void getLargestWriteTimeStampTest() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(originTable.indexOf("WRITETIME("+writetimeColumnName+")")).thenReturn(100);
        when(originRow.getLong(eq(100))).thenReturn(1000L);
        when(originTable.indexOf("WRITETIME("+writetimeTTLColumnName+")")).thenReturn(101);
        when(originRow.getLong(eq(101))).thenReturn(3000L);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        Long largestWritetime = feature.getLargestWriteTimeStamp(originRow);
        assertEquals(3000L, largestWritetime);
    }

    @Test
    public void getLargestWriteTimeStampWithListTest() {
        when(propertyHelper.getBoolean(KnownProperties.ALLOW_COLL_FOR_WRITETIME_TTL_CALC)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(originTable.indexOf("WRITETIME("+writetimeColumnName+")")).thenReturn(100);
        when(originRow.getType(eq(100))).thenReturn(DataTypes.listOf(DataTypes.BIGINT));
        when(originRow.getList(eq(100), eq(BigInteger.class))).thenReturn(Arrays.asList(BigInteger.valueOf(4001), BigInteger.valueOf(2001)));
        when(originTable.indexOf("WRITETIME("+writetimeTTLColumnName+")")).thenReturn(101);
        when(originRow.getType(eq(101))).thenReturn(DataTypes.BIGINT);
        when(originRow.getLong(eq(101))).thenReturn(1000l);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        Long largestWritetime = feature.getLargestWriteTimeStamp(originRow);
        assertEquals(4001L, largestWritetime);
    }

    @Test
    public void getLargestWriteTimeStampWithCustomTimeTest() {
        when(originTable.indexOf("WRITETIME("+writetimeColumnName+")")).thenReturn(100);
        when(originRow.getLong(eq(100))).thenReturn(1000L);
        when(originTable.indexOf("WRITETIME("+writetimeTTLColumnName+")")).thenReturn(101);
        when(originRow.getLong(eq(101))).thenReturn(3000L);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        Long largestWritetime = feature.getLargestWriteTimeStamp(originRow);
        assertEquals(3000L, largestWritetime);
    }

    @Test
    public void getLargestTTLTest() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(null);
        when(originTable.indexOf("TTL("+ttlColumnName+")")).thenReturn(100);
        when(originRow.getInt(eq(100))).thenReturn(30);
        when(originTable.indexOf("TTL("+writetimeTTLColumnName+")")).thenReturn(101);
        when(originRow.getInt(eq(101))).thenReturn(20);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        Integer largestTTL = feature.getLargestTTL(originRow);
        assertEquals(30, largestTTL);
    }

    @Test
    public void getLargestTTLWithListTest() {
        when(propertyHelper.getBoolean(KnownProperties.ALLOW_COLL_FOR_WRITETIME_TTL_CALC)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL)).thenReturn(null);
        when(originTable.indexOf("TTL("+ttlColumnName+")")).thenReturn(100);
        when(originRow.getType(eq(100))).thenReturn(DataTypes.listOf(DataTypes.INT));
        when(originRow.getList(eq(100), eq(Integer.class))).thenReturn(Arrays.asList(Integer.valueOf(40), Integer.valueOf(10)));
        when(originTable.indexOf("TTL("+writetimeTTLColumnName+")")).thenReturn(101);
        when(originRow.getType(eq(101))).thenReturn(DataTypes.INT);
        when(originRow.getInt(eq(101))).thenReturn(20);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        Integer largestTTL = feature.getLargestTTL(originRow);
        assertEquals(40, largestTTL);
    }

    @Test
    public void validateInvalidFilterMin() {
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(-1L);
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void validateInvalidFilterMax() {
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(-1L);
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void validateInvalidFilterMinMax() {
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(200L);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(100L);
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void validateInvalidCustomWritetime() {
        // negative custom writetime is reset to zero and the feature remains enabled
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(-1L);
        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertTrue(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate"),
                () -> assertEquals(0L, feature.getCustomWritetime(), "custom writetime should be set to safe value 0")
        );
    }

    @Test
    public void validateInvalidTTLColumn() {
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(Collections.singletonList("invalid"));
        when(originTable.indexOf("invalid")).thenReturn(-1);
        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void validateEmptyTTLColumn() {
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(Collections.emptyList());
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void validateInvalidCustomWritetimeColumn() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(Collections.singletonList("invalid"));
        when(originTable.indexOf("invalid")).thenReturn(-1);
        assertAll(
                () -> assertTrue(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void validateEmptyWritetimeColumn() {
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(Collections.emptyList());
        assertAll(
                () -> assertFalse(feature.loadProperties(propertyHelper), "loadProperties"),
                () -> assertFalse(feature.initializeAndValidate(originTable, targetTable), "initializeAndValidate")
        );
    }

    @Test
    public void disabledWhenFilterWithNoWritetime() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(false);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(filterMin);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(filterMax);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertFalse(feature.isEnabled());
    }

    @Test
    public void testOriginIsNull() {
        feature.loadProperties(propertyHelper);
        assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(null, targetTable));
    }

    @Test
    public void testOriginIsTarget() {
        when(originTable.isOrigin()).thenReturn(false);
        feature.loadProperties(propertyHelper);
        assertThrows(IllegalArgumentException.class, () -> feature.initializeAndValidate(originTable, targetTable));
    }

    @Test
    public void writetimeIncrementTest() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME_INCREMENT)).thenReturn(314L);
        when(originTable.indexOf("WRITETIME("+writetimeColumnName+")")).thenReturn(100);
        when(originRow.getLong(eq(100))).thenReturn(1000L);
        when(originTable.indexOf("WRITETIME("+writetimeTTLColumnName+")")).thenReturn(101);
        when(originRow.getLong(eq(101))).thenReturn(3000L);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        Long largestWritetime = feature.getLargestWriteTimeStamp(originRow);
        assertEquals(3314L, largestWritetime);
    }

    @Test
    public void invalidWritetimeIncrement() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME_INCREMENT)).thenReturn(-1L);
        feature.loadProperties(propertyHelper);
        assertFalse(feature.isEnabled());
    }

    @Test
    public void testZeroIncrementWithUnfrozenList() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME_INCREMENT)).thenReturn(0L);
        when(originTable.hasUnfrozenList()).thenReturn(true);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        assertTrue(feature.isEnabled());
    }

    @Test
    public void customWriteTime_withAutoWritetime() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(12345L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(false);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);

        assertAll(
                () -> assertFalse(feature.hasWriteTimestampFilter(), "hasWriteTimestampFilter"),
                () -> assertTrue(feature.hasWritetimeColumns(), "hasWritetimeColumns"),
                () -> assertEquals(12345L, feature.getCustomWritetime(), "hasWritetimeColumns")
        );
    }

    @Test
    public void specifiedColumnsOverrideAuto(){
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(Arrays.<String>asList("writetime_ttl_col"));
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(Arrays.<String>asList("writetime_ttl_col"));
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(true);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        assertAll(
                () -> assertTrue(feature.hasTTLColumns(), "has TTL columns"),
                () -> assertTrue(feature.hasWritetimeColumns(), "has Writetime columns"),
                () -> assertEquals(1, feature.getWritetimeNames().size(), "has exactly 1 Writetime column"),
                () -> assertEquals(1, feature.getTtlNames().size(), "has exactly 1 TTL column"),
                () -> assertTrue(feature.isEnabled, "feature is enabled")
        );

        //gets called once for adding TTL(..) columns  and once for adding WRITETIME(..) columns
        verify(originTable, times(2)).extendColumns(any(),any());
    }

    @Test
    public void autoFlagResultsInAllValueColumnsBeingUsed(){
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO)).thenReturn(true);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN)).thenReturn(null);
        when(propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX)).thenReturn(null);
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES)).thenReturn(null);
        when(propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO)).thenReturn(true);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        assertAll(
                () -> assertTrue(feature.hasTTLColumns(), "has TTL columns"),
                () -> assertTrue(feature.hasWritetimeColumns(), "has Writetime columns"),
                () -> assertEquals(3, feature.getWritetimeNames().size(), "has exactly 1 Writetime column"),
                () -> assertEquals(3, feature.getTtlNames().size(), "has exactly 1 TTL column"),
                () -> assertTrue(feature.isEnabled, "feature is enabled")
        );

        //gets called once for adding TTL(..) columns  and once for adding WRITETIME(..) columns
        verify(originTable, times(2)).extendColumns(any(),any());
    }
}
