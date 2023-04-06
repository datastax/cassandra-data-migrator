package datastax.astra.migrate.cql.features;

public class FeatureFactory {
    public static Feature getFeature(Featureset feature) {
        switch (feature) {
            case ORIGIN_FILTER: return new OriginFilterCondition();
            default:
                throw new IllegalArgumentException("Unknown feature: " + feature);
        }
    }
}
