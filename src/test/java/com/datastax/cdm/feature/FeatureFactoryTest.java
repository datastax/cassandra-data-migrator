package com.datastax.cdm.feature;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FeatureFactoryTest {

    @Test
    public void knownButUnimplementedFeature() {
        assertThrows(IllegalArgumentException.class, () -> FeatureFactory.getFeature(Featureset.TEST_UNIMPLEMENTED_FEATURE));
    }

    @Test
    public void testKnownFeatures() {
        int expectedFeatures = 0;
        Map<Featureset,Feature> featureMap = new HashMap<>();
        for (Featureset feature : Featureset.values()) {
            if (Featureset.TEST_UNIMPLEMENTED_FEATURE.equals(feature)) continue;
            featureMap.put(feature, FeatureFactory.getFeature(feature));
            expectedFeatures++;
        }

        // assert that the size of the featureList matches the number of expected features
        assertEquals(expectedFeatures, featureMap.size(), "all features should be added");

        // assert that none of the features in the list are null
        assertAll(
                featureMap.entrySet().stream()
                        .map(entry -> () -> assertNotNull(entry.getValue(), "Feature is null for key " + entry.getKey()))
        );
    }

    @Test
    public void testIsEnabled() {
        Feature mockEnabledFeature = mock(Feature.class);
        when(mockEnabledFeature.isEnabled()).thenReturn(true);
        Feature mockDisabledFeature = mock(Feature.class);
        when(mockDisabledFeature.isEnabled()).thenReturn(false);

        assertAll(
                () -> assertFalse(FeatureFactory.isEnabled(null), "null feature should return false"),
                () -> assertFalse(FeatureFactory.isEnabled(mockDisabledFeature), "feature should return false"),
                () -> assertTrue(FeatureFactory.isEnabled(mockEnabledFeature), "feature should return true")
                );
    }


}
