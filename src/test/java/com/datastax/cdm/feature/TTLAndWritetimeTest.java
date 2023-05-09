package com.datastax.cdm.feature;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TTLAndWritetimeTest extends CommonMocks {

    WritetimeTTL feature;
    Long customWritetime = 123456789L;
    Long filterMin = 100000000L;
    Long filterMax = 200000000L;
    String writetimeColumnName = "writetime_col";
    String ttlColumnName = "ttl_col";
    String writetimeTTLColumnName = "writetime_ttl_col";

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        setTestVariables();
        commonSetupWithoutDefaultClassVariables(false,false,false);
        setTestWhens();
        feature = new WritetimeTTL();
    }

    private void setTestVariables() {
        originValueColumns = new ArrayList<>();
        originValueColumns.addAll(Arrays.asList(writetimeColumnName,ttlColumnName,writetimeTTLColumnName));
        originValueColumnTypes = new ArrayList<>(originValueColumnTypes);
        originValueColumnTypes.addAll(Arrays.asList(DataTypes.TEXT,DataTypes.TEXT,DataTypes.TEXT));
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
        when(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES)).thenReturn(null);

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
    public void smoke_writetimeWithoutTTL() {
        when(propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME)).thenReturn(0L);
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
                () -> assertTrue(feature.isValid, "isValid")
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
    public void getLargestWriteTimeStampWithCustomTimeTest() {
        when(originTable.indexOf("WRITETIME("+writetimeColumnName+")")).thenReturn(100);
        when(originRow.getLong(eq(100))).thenReturn(1000L);
        when(originTable.indexOf("WRITETIME("+writetimeTTLColumnName+")")).thenReturn(101);
        when(originRow.getLong(eq(101))).thenReturn(3000L);

        feature.loadProperties(propertyHelper);
        feature.initializeAndValidate(originTable, targetTable);
        Long largestWritetime = feature.getLargestWriteTimeStamp(originRow);
        assertEquals(customWritetime, largestWritetime);
    }

    @Test
    public void getLargestTTLTest() {
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
}
