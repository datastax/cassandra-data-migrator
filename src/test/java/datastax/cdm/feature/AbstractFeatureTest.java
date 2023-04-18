package datastax.cdm.feature;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.properties.PropertyHelper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class AbstractFeatureTest {
    enum TestProperty {
        STRING,
        NUMBER,
        BOOLEAN,
        MIGRATE_DATA_TYPE,
        STRING_LIST,
        NUMBER_LIST,
        MIGRATE_DATA_TYPE_LIST
    }
    class TestFeature extends AbstractFeature {
        @Override
        public boolean initialize(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
            isEnabled = true;
            isInitialized = true;
            return true;
        }

        public boolean initialize_withValues() {
            putString(TestProperty.STRING, "test");
            putNumber(TestProperty.NUMBER, Integer.MAX_VALUE);
            putBoolean(TestProperty.BOOLEAN, true);
            putMigrateDataType(TestProperty.MIGRATE_DATA_TYPE, new MigrateDataType("0"));
            putStringList(TestProperty.STRING_LIST, Arrays.asList("a","b","c"));
            putNumberList(TestProperty.NUMBER_LIST, Arrays.asList(1,2,3));
            putMigrateDataTypeList(TestProperty.MIGRATE_DATA_TYPE_LIST, Arrays.asList(new MigrateDataType("0"), new MigrateDataType("1"), new MigrateDataType("2")));
            isEnabled = true;
            isInitialized = true;
            return true;
        }
    }

    @Test
    public void smokeTest() {
        TestFeature testFeature = new TestFeature();
        testFeature.initialize_withValues();
        assertAll(
                () -> assertEquals("test", testFeature.getString(TestProperty.STRING)),
                () -> assertEquals(Integer.MAX_VALUE, testFeature.getNumber(TestProperty.NUMBER)),
                () -> assertEquals(Integer.MAX_VALUE, testFeature.getInteger(TestProperty.NUMBER)),
                () -> assertTrue(testFeature.getBoolean(TestProperty.BOOLEAN)),
                () -> assertEquals(new MigrateDataType("0"), testFeature.getMigrateDataType(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertEquals(Arrays.asList("a","b","c"), testFeature.getStringList(TestProperty.STRING_LIST)),
                () -> assertEquals(Arrays.asList(1,2,3), testFeature.getNumberList(TestProperty.NUMBER_LIST)),
                () -> assertEquals(Arrays.asList(1,2,3), testFeature.getIntegerList(TestProperty.NUMBER_LIST)),
                () -> assertEquals(Arrays.asList(new MigrateDataType("0"), new MigrateDataType("1"), new MigrateDataType("2")), testFeature.getMigrateDataTypeList(TestProperty.MIGRATE_DATA_TYPE_LIST)),
                () -> assertTrue(testFeature.isEnabled())
        );
    }

    @Test
    public void asString() {
        TestFeature testFeature = new TestFeature();
        testFeature.initialize_withValues();
        assertAll(
                () -> assertEquals("test", testFeature.getAsString(TestProperty.STRING)),
                () -> assertEquals("2147483647", testFeature.getAsString(TestProperty.NUMBER)),
                () -> assertEquals("true", testFeature.getAsString(TestProperty.BOOLEAN)),
                () -> assertEquals("0", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertEquals("a,b,c", testFeature.getAsString(TestProperty.STRING_LIST)),
                () -> assertEquals("1,2,3", testFeature.getAsString(TestProperty.NUMBER_LIST)),
                () -> assertEquals("0,1,2", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE_LIST))
        );
    }

    @Test
    public void isEnabled_whenNotInitialized() {
        TestFeature testFeature = new TestFeature();
        assertThrows(RuntimeException.class, testFeature::isEnabled);
    }

    @Test
    public void unsetValue() {
        TestFeature testFeature = new TestFeature();
        testFeature.initialize(null, null);
        assertAll(
                () -> assertNull(testFeature.getString(TestProperty.STRING)),
                () -> assertNull(testFeature.getNumber(TestProperty.NUMBER)),
                () -> assertNull(testFeature.getBoolean(TestProperty.BOOLEAN)),
                () -> assertNull(testFeature.getMigrateDataType(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertNull(testFeature.getStringList(TestProperty.STRING_LIST)),
                () -> assertNull(testFeature.getNumberList(TestProperty.NUMBER_LIST)),
                () -> assertNull(testFeature.getMigrateDataTypeList(TestProperty.MIGRATE_DATA_TYPE_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.STRING)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.NUMBER)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.BOOLEAN)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.STRING_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.NUMBER_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE_LIST))
        );
    }

    @Test
    public void valuesSetToNull() {
        TestFeature testFeature = new TestFeature();
        testFeature.initialize(null, null);
        testFeature.putString(TestProperty.STRING, null);
        testFeature.putNumber(TestProperty.NUMBER, null);
        testFeature.putBoolean(TestProperty.BOOLEAN, null);
        testFeature.putMigrateDataType(TestProperty.MIGRATE_DATA_TYPE, null);
        testFeature.putStringList(TestProperty.STRING_LIST, null);
        testFeature.putNumberList(TestProperty.NUMBER_LIST, null);
        testFeature.putMigrateDataTypeList(TestProperty.MIGRATE_DATA_TYPE_LIST, null);

        assertAll(
                () -> assertNull(testFeature.getString(TestProperty.STRING)),
                () -> assertNull(testFeature.getNumber(TestProperty.NUMBER)),
                () -> assertNull(testFeature.getBoolean(TestProperty.BOOLEAN)),
                () -> assertNull(testFeature.getMigrateDataType(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertNull(testFeature.getStringList(TestProperty.STRING_LIST)),
                () -> assertNull(testFeature.getNumberList(TestProperty.NUMBER_LIST)),
                () -> assertNull(testFeature.getMigrateDataTypeList(TestProperty.MIGRATE_DATA_TYPE_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.STRING)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.NUMBER)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.BOOLEAN)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.STRING_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.NUMBER_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE_LIST))
        );
    }

    @Test
    public void valueSet_butDisabled() {
        TestFeature testFeature = new TestFeature();
        testFeature.initialize_withValues();
        testFeature.isEnabled = false;
        assertAll(
                () -> assertNull(testFeature.getString(TestProperty.STRING)),
                () -> assertNull(testFeature.getNumber(TestProperty.NUMBER)),
                () -> assertNull(testFeature.getBoolean(TestProperty.BOOLEAN)),
                () -> assertNull(testFeature.getMigrateDataType(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertNull(testFeature.getStringList(TestProperty.STRING_LIST)),
                () -> assertNull(testFeature.getNumberList(TestProperty.NUMBER_LIST)),
                () -> assertNull(testFeature.getMigrateDataTypeList(TestProperty.MIGRATE_DATA_TYPE_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.STRING)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.NUMBER)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.BOOLEAN)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.STRING_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.NUMBER_LIST)),
                () -> assertEquals("", testFeature.getAsString(TestProperty.MIGRATE_DATA_TYPE_LIST)),
                () -> assertEquals("test", testFeature.getRawString(TestProperty.STRING)),
                () -> assertEquals(Integer.MAX_VALUE, testFeature.getRawNumber(TestProperty.NUMBER)),
                () -> assertEquals(Integer.MAX_VALUE, testFeature.getRawInteger(TestProperty.NUMBER)),
                () -> assertTrue(testFeature.getRawBoolean(TestProperty.BOOLEAN)),
                () -> assertEquals(new MigrateDataType("0"), testFeature.getRawMigrateDataType(TestProperty.MIGRATE_DATA_TYPE)),
                () -> assertEquals(Arrays.asList("a","b","c"), testFeature.getRawStringList(TestProperty.STRING_LIST)),
                () -> assertEquals(Arrays.asList(1,2,3), testFeature.getRawNumberList(TestProperty.NUMBER_LIST)),
                () -> assertEquals(Arrays.asList(1,2,3), testFeature.getRawIntegerList(TestProperty.NUMBER_LIST)),
                () -> assertEquals(Arrays.asList(new MigrateDataType("0"), new MigrateDataType("1"), new MigrateDataType("2")), testFeature.getRawMigrateDataTypeList(TestProperty.MIGRATE_DATA_TYPE_LIST))
        );
    }

    @Test
    public void alterProperties_Null() {
        TestFeature testFeature = new TestFeature();
        testFeature.alterProperties(null, null);
        assertNull(testFeature.alterProperties(null, null));
    }
}
