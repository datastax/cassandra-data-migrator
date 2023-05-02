package com.datastax.cdm.feature;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class FeatureFactoryTest {

    @Test
    public void knownButUnimplementedFeature() {
        assertThrows(IllegalArgumentException.class, () -> FeatureFactory.getFeature(Featureset.TEST_UNIMPLEMENTED_FEATURE));
    }
}
