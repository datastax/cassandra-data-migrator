package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class OriginFilterConditionTest {

    PropertyHelper helper;
    SparkConf validSparkConf;
    Feature feature;

    @BeforeEach
    public void setup() {
        helper = PropertyHelper.getInstance();
        validSparkConf = new SparkConf();
        feature = FeatureFactory.getFeature(Featureset.ORIGIN_FILTER);
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
        validSparkConf = null;
    }

    @Test
    public void smokeTest() {
        String conditionIn = "AND a > 1";
        validSparkConf.set(KnownProperties.ORIGIN_FILTER_CONDITION, conditionIn);
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);

        String conditionOut = feature.getString(OriginFilterCondition.Property.CONDITION);

        assertAll(
                () -> assertTrue(conditionOut.getClass() == String.class),
                () -> assertEquals(conditionIn, conditionOut)
        );
    }

    @Test
    public void andIsPrepended() {
        String conditionIn = "a > 1";
        validSparkConf.set(KnownProperties.ORIGIN_FILTER_CONDITION, conditionIn);
        helper.initializeSparkConf(validSparkConf);
        feature.initialize(helper);

        String conditionOut = feature.getString(OriginFilterCondition.Property.CONDITION);
        assertEquals("AND "+conditionIn, conditionOut.trim());
    }
    
}
