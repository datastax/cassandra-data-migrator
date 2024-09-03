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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;

class AbstractFeatureTest {

    private static class TestFeature extends AbstractFeature {

        @Override
        public boolean loadProperties(IPropertyHelper propertyHelper) {
            isLoaded = true;
            return isLoaded;
        }

        protected void setIsValid(boolean isValid) {
            this.isValid = isValid;
        }
    }

    private TestFeature testFeature;
    private IPropertyHelper propertyHelper;
    private CqlTable originTable;
    private CqlTable targetTable;

    @BeforeEach
    void setUp() {
        testFeature = new TestFeature();
        propertyHelper = Mockito.mock(IPropertyHelper.class);
        originTable = Mockito.mock(CqlTable.class);
        targetTable = Mockito.mock(CqlTable.class);
    }

    @Test
    void initialize_setsInitializedToTrueAndReturnsTrue() {
        boolean result = testFeature.loadProperties(propertyHelper);
        assertAll(() -> assertTrue(result, "Expected initialize() to return true"),
                () -> assertTrue(testFeature.isLoaded, "Expected isInitialized to be set to true"));
    }

    @Test
    void isEnabled_initialized_returnsFalse() {
        testFeature.loadProperties(propertyHelper);
        assertFalse(testFeature.isEnabled());
    }

    @Test
    void isEnabled_notInitialized_throwsRuntimeException() {
        assertThrows(RuntimeException.class, () -> testFeature.isEnabled());
    }

    @Test
    void validate_initialized_returnsTrue() {
        testFeature.loadProperties(propertyHelper);
        assertTrue(testFeature.initializeAndValidate(originTable, targetTable));
    }

    @Test
    void validate_notInitialized_throwsRuntimeException() {
        assertThrows(RuntimeException.class, () -> testFeature.initializeAndValidate(originTable, targetTable));
    }

    @Test
    void validate_invalidProperties_returnsFalse() {
        testFeature.loadProperties(propertyHelper);
        testFeature.setIsValid(false);
        assertFalse(testFeature.initializeAndValidate(originTable, targetTable));
    }
}
