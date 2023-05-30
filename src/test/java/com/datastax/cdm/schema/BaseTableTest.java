package com.datastax.cdm.schema;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.properties.KnownProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

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
                () -> assertEquals("origin_ks", bt.getKeyspaceName()),
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
        assertTrue(thrown.getMessage().contentEquals("Value for required property " + KnownProperties.ORIGIN_KEYSPACE_TABLE + " now provided!!"));
    }
}