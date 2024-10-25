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
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.cdm.cql.CommonMocks;
import com.datastax.cdm.properties.KnownProperties;

public class GuardrailTest extends CommonMocks {
    Guardrail guardrail;
    Integer colSizeInKB = 100;

    @BeforeEach
    public void setup() {
        defaultClassVariables();
        commonSetupWithoutDefaultClassVariables(false, false, false);
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(null); // unconfigured
        guardrail = new Guardrail();
    }

    @Test
    public void smoke_cleanCheck() {
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(colSizeInKB);

        guardrail.loadProperties(propertyHelper);
        guardrail.initializeAndValidate(originTable, targetTable);

        String guardrailChecksResult = guardrail.guardrailChecks(originRow);
        assertEquals(Guardrail.CLEAN_CHECK, guardrailChecksResult, "guardrailChecks");
    }

    @Test
    public void smoke_nullWhenDisabled() {
        guardrail.loadProperties(propertyHelper);
        guardrail.initializeAndValidate(originTable, targetTable);

        String guardrailChecksResult = guardrail.guardrailChecks(originRow);
        assertNull(guardrailChecksResult, "guardrailChecks");
    }

    @Test
    public void smoke_exceedCheck() {
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(1);
        guardrail.loadProperties(propertyHelper);
        guardrail.initializeAndValidate(originTable, targetTable);

        when(originTable.byteCount(anyInt(),any())).thenReturn(Guardrail.BASE_FACTOR+1);

        String guardrailChecksResult = guardrail.guardrailChecks(originRow);
        assertTrue(guardrailChecksResult.startsWith("Large columns"), "guardrailChecks");
    }

    @Test
    public void smoke_explodeMap() {
        defaultClassVariables();
        commonSetupWithoutDefaultClassVariables(true, false, false);
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(1);
        guardrail.loadProperties(propertyHelper);
        guardrail.initializeAndValidate(originTable, targetTable);

        String guardrailChecksResult = guardrail.guardrailChecks(originRow);
        assertEquals(Guardrail.CLEAN_CHECK, guardrailChecksResult, "guardrailChecks");
    }

    @Test
    public void loadProperties_configured() {
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(colSizeInKB);
        boolean loadPropertiesResult = guardrail.loadProperties(propertyHelper);

        assertAll(
                () -> assertTrue(loadPropertiesResult, "loadProperties"),
                () -> assertTrue(guardrail.isEnabled(), "enabled")
        );
    }

    @Test
    public void loadProperties_unconfigured() {
        boolean loadPropertiesResult = guardrail.loadProperties(propertyHelper);

        assertAll(() -> assertTrue(loadPropertiesResult, "loadProperties"),
                () -> assertFalse(guardrail.isEnabled(), "enabled"));
    }

    @Test
    public void loadProperties_invalidProperty() {
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(-1);
        boolean loadPropertiesResult = guardrail.loadProperties(propertyHelper);

        assertAll(
                () -> assertFalse(loadPropertiesResult, "loadProperties"),
                () -> assertFalse(guardrail.isEnabled(), "not enabled")
        );
    }

    @Test
    public void initializeAndValidate() {
        when(originTable.isOrigin()).thenReturn(true);
        when(targetTable.isOrigin()).thenReturn(false);

        guardrail.loadProperties(propertyHelper);
        boolean initializeAndValidateResult = guardrail.initializeAndValidate(originTable, targetTable);

        assertTrue(initializeAndValidateResult, "initializeAndValidate");
    }

    @Test
    public void initializeAndValidate_invalidOrigin() {
        when(originTable.isOrigin()).thenReturn(false);
        when(targetTable.isOrigin()).thenReturn(false);

        guardrail.loadProperties(propertyHelper);
        boolean initializeAndValidateResult = guardrail.initializeAndValidate(originTable, targetTable);

        assertFalse(initializeAndValidateResult, "initializeAndValidate");
    }

    @Test
    public void initializeAndValidate_invalidTarget() {
        when(originTable.isOrigin()).thenReturn(false);

        guardrail.loadProperties(propertyHelper);
        boolean initializeAndValidateResult = guardrail.initializeAndValidate(originTable, targetTable);

        assertFalse(initializeAndValidateResult, "initializeAndValidate");
    }

    @Test
    public void initializeAndValidate_invalidConfig() {
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(-1);

        guardrail.loadProperties(propertyHelper);
        boolean initializeAndValidateResult = guardrail.initializeAndValidate(originTable, targetTable);

        assertFalse(initializeAndValidateResult, "initializeAndValidate");
    }

    @Test
    public void checkWhenDisabled() {
        guardrail.loadProperties(propertyHelper);
        guardrail.initializeAndValidate(originTable, targetTable);

        String guardrailChecksResult = guardrail.guardrailChecks(originRow);
        assertNull(guardrailChecksResult, "guardrailChecks");
    }

    @Test
    public void checkWithNullRecord() {
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(colSizeInKB);
        guardrail.loadProperties(propertyHelper);
        guardrail.initializeAndValidate(originTable, targetTable);

        String guardrailChecksResult = guardrail.guardrailChecks(null);
        assertEquals(Guardrail.CLEAN_CHECK, guardrailChecksResult, "guardrailChecks");
    }

    @Test
    public void checkWithNullOriginRow() {
        when(propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB)).thenReturn(colSizeInKB);
        when(record.getOriginRow()).thenReturn(null);

        guardrail.loadProperties(propertyHelper);
        guardrail.initializeAndValidate(originTable, targetTable);

        String guardrailChecksResult = guardrail.guardrailChecks(originRow);
        assertEquals(Guardrail.CLEAN_CHECK, guardrailChecksResult, "guardrailChecks");
    }

}
