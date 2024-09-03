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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class FeatureFactoryTest {

    @Test
    public void knownButUnimplementedFeature() {
        assertThrows(IllegalArgumentException.class,
                () -> FeatureFactory.getFeature(Featureset.TEST_UNIMPLEMENTED_FEATURE));
    }

    @Test
    public void testKnownFeatures() {
        int expectedFeatures = 0;
        Map<Featureset, Feature> featureMap = new HashMap<>();
        for (Featureset feature : Featureset.values()) {
            if (Featureset.TEST_UNIMPLEMENTED_FEATURE.equals(feature))
                continue;
            featureMap.put(feature, FeatureFactory.getFeature(feature));
            expectedFeatures++;
        }

        // assert that the size of the featureList matches the number of expected features
        assertEquals(expectedFeatures, featureMap.size(), "all features should be added");

        // assert that none of the features in the list are null
        assertAll(featureMap.entrySet().stream()
                .map(entry -> () -> assertNotNull(entry.getValue(), "Feature is null for key " + entry.getKey())));
    }

    @Test
    public void testIsEnabled() {
        Feature mockEnabledFeature = mock(Feature.class);
        when(mockEnabledFeature.isEnabled()).thenReturn(true);
        Feature mockDisabledFeature = mock(Feature.class);
        when(mockDisabledFeature.isEnabled()).thenReturn(false);

        assertAll(() -> assertFalse(FeatureFactory.isEnabled(null), "null feature should return false"),
                () -> assertFalse(FeatureFactory.isEnabled(mockDisabledFeature), "feature should return false"),
                () -> assertTrue(FeatureFactory.isEnabled(mockEnabledFeature), "feature should return true"));
    }

}
