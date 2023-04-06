package datastax.astra.migrate.cql.features;

public class FeatureFactory {
    public static Feature getFeature(Featureset feature) {
        switch (feature) {
            case ORIGIN_FILTER: return new OriginFilterCondition();
            case CONSTANT_COLUMNS: return new ConstantColumns();
            default:
                throw new IllegalArgumentException("Unknown feature: " + feature);
        }
    }
}
