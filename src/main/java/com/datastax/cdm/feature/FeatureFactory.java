package com.datastax.cdm.feature;

public class FeatureFactory {
    public static Feature getFeature(Featureset feature) {
        switch (feature) {
            case ORIGIN_FILTER: return new OriginFilterCondition();
            case CONSTANT_COLUMNS: return new ConstantColumns();
            case EXPLODE_MAP: return new ExplodeMap();
            case WRITETIME_TTL: return new WritetimeTTL();
            default:
                throw new IllegalArgumentException("Unknown feature: " + feature);
        }
    }

    public static Boolean isEnabled(Feature f) {
        return null != f && f.isEnabled();
    }
}
