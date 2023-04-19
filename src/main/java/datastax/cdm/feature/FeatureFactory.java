package datastax.cdm.feature;

public class FeatureFactory {
    public static Feature getFeature(Featureset feature) {
        switch (feature) {
            case ORIGIN_FILTER: return new OriginFilterCondition();
            case CONSTANT_COLUMNS: return new ConstantColumns();
            case EXPLODE_MAP: return new ExplodeMap();
            case UDT_MAPPER: return new UDTMapper();
            case WRITETIME_TTL_COLUMN: return new WritetimeTTLColumn();
            default:
                throw new IllegalArgumentException("Unknown feature: " + feature);
        }
    }

    public static Boolean isEnabled(Feature f) {
        return null != f && f.isEnabled();
    }
}
