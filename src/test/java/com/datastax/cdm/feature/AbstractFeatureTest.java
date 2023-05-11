package com.datastax.cdm.feature;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

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
        assertAll(
                () -> assertTrue(result, "Expected initialize() to return true"),
                () -> assertTrue(testFeature.isLoaded, "Expected isInitialized to be set to true")
        );
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
