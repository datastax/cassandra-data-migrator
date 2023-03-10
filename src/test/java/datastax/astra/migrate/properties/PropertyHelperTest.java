package datastax.astra.migrate.properties;

import java.util.List;
import java.util.Arrays;

import datastax.astra.migrate.MigrateDataType;
import org.apache.spark.SparkConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PropertyHelperTest {
    PropertyHelper helper;
    @Before
    public void setup() {
        helper = PropertyHelper.getInstance();
    }

    @After
    public void tearDown() {
        PropertyHelper.destroyInstance();
    }

    @Test
    public void setProperty_String() {
        String value = "test_value";
        String setValue = (String) helper.setProperty(KnownProperties.TEST_STRING, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_StringList() {
        List<String> value = Arrays.asList("a","b", "c");
        List<String> setValue = (List<String>) helper.setProperty(KnownProperties.TEST_STRING_LIST, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_StringList_empty() {
        List<String> value = Arrays.asList();
        List<String> setValue = (List<String>) helper.setProperty(KnownProperties.TEST_STRING_LIST, value);
        assertNull(setValue);
    }

    @Test
    public void setProperty_Number() {
        Integer value = 1234;
        Integer setValue = (Integer) helper.setProperty(KnownProperties.TEST_NUMBER, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_NumberList() {
        List<Integer> value = Arrays.asList(1,2,3,4);
        List<Integer> setValue = (List<Integer>) helper.setProperty(KnownProperties.TEST_NUMBER_LIST, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_NumberList_empty() {
        List<String> value = Arrays.asList();
        List<Integer> setValue = (List<Integer>) helper.setProperty(KnownProperties.TEST_NUMBER_LIST, value);
        assertNull(setValue);
    }

    @Test
    public void setProperty_Boolean() {
        Boolean value = true;
        Boolean setValue = (Boolean) helper.setProperty(KnownProperties.TEST_BOOLEAN, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_MigrateDataType() {
        MigrateDataType value = new MigrateDataType("1");
        MigrateDataType setValue = (MigrateDataType) helper.setProperty(KnownProperties.TEST_MIGRATE_TYPE, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_MigrateDataTypeList() {
        List<MigrateDataType> value = Arrays.asList(new MigrateDataType("1"),new MigrateDataType("2"));
        List<MigrateDataType> setValue = (List<MigrateDataType>) helper.setProperty(KnownProperties.TEST_MIGRATE_TYPE_LIST, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_MigrateDataTypeList_empty() {
        List<MigrateDataType> value = Arrays.asList();
        List<MigrateDataType> setValue = (List<MigrateDataType>) helper.setProperty(KnownProperties.TEST_MIGRATE_TYPE_LIST, value);
        assertNull(setValue);
    }

    @Test
    public void setProperty_nullArguments() {
        assertNull(helper.setProperty(null, "test"));
        assertNull(helper.setProperty(KnownProperties.TEST_STRING, null));
    }

    @Test
    public void setProperty_mismatchType() {
        assertNull(helper.setProperty(KnownProperties.TEST_STRING, Integer.valueOf(1)));
    }

    @Test
    public void setProperty_unknownType() {
        assertNull(helper.setProperty(KnownProperties.TEST_UNKNOWN, "abc"));
    }

    @Test
    public void getString() {
        helper.setProperty(KnownProperties.TEST_STRING, "test");
        assertEquals("test", helper.getString(KnownProperties.TEST_STRING));
    }

    @Test
    public void getStringList() {
        helper.setProperty(KnownProperties.TEST_STRING_LIST, Arrays.asList("a","b","c"));
        assertEquals(Arrays.asList("a","b","c"), helper.getStringList(KnownProperties.TEST_STRING_LIST));
    }

    @Test
    public void getNumber() {
        helper.setProperty(KnownProperties.TEST_NUMBER, 4321);
        assertEquals(4321, helper.getNumber(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getInteger_Integer() {
        helper.setProperty(KnownProperties.TEST_NUMBER, 1234);
        assertEquals(Integer.valueOf(1234), helper.getInteger(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getInteger_Short() {
        helper.setProperty(KnownProperties.TEST_NUMBER, Short.MAX_VALUE);
        assertEquals(Integer.valueOf(Short.MAX_VALUE), helper.getInteger(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getInteger_Byte() {
        helper.setProperty(KnownProperties.TEST_NUMBER, Byte.MAX_VALUE);
        assertEquals(Integer.valueOf(Byte.MAX_VALUE), helper.getInteger(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getInteger_Long_smallValue() {
        helper.setProperty(KnownProperties.TEST_NUMBER, Long.parseLong("1234"));
        assertEquals(Integer.valueOf(1234), helper.getInteger(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getInteger_Long_tooBigForInteger() {
        helper.setProperty(KnownProperties.TEST_NUMBER, Long.MIN_VALUE);
        assertNull(helper.getInteger(KnownProperties.TEST_NUMBER));

        helper.setProperty(KnownProperties.TEST_NUMBER, Long.MAX_VALUE);
        assertNull(helper.getInteger(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getInteger_nullArgument() {
        assertNull(helper.getInteger(null));
    }

    @Test
    public void getInteger_wrongType() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1,2,3));
        assertNull(helper.getInteger(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getLong() {
        helper.setProperty(KnownProperties.TEST_NUMBER, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, (long) helper.getLong(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getLong_nullArgument() {
        assertNull(helper.getLong(null));
    }

    @Test
    public void getLong_wrongType() {
        helper.setProperty(KnownProperties.TEST_STRING, String.valueOf(Long.MAX_VALUE));
        assertNull(helper.getLong(KnownProperties.TEST_STRING));
    }

    @Test
    public void getNumberList() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1,2,3));
        assertEquals(Arrays.asList(1,2,3), helper.getNumberList(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getIntegerList() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1,2,3));
        assertEquals(Arrays.asList(1,2,3), helper.getIntegerList(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getIntegerList_nullParameter() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1,2,3));
        assertNull(helper.getIntegerList(null));
    }

    @Test
    public void getIntegerList_wrongType() {
        helper.setProperty(KnownProperties.TEST_STRING_LIST, Arrays.asList("1","2","3"));
        assertNull(helper.getIntegerList(KnownProperties.TEST_STRING_LIST));
    }

    @Test
    public void getIntegerList_notAllValuesFit() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1, Double.MAX_VALUE, 3));
        assertNull(helper.getIntegerList(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getIntegerList_noListSet() {
        assertNull(helper.getIntegerList(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getIntegerList_nullValueInList() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1, null, 3));
        assertNull(helper.getIntegerList(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getBoolean() {
        helper.setProperty(KnownProperties.TEST_BOOLEAN, false);
        assertFalse(helper.getBoolean(KnownProperties.TEST_BOOLEAN));
    }

    @Test
    public void getMigrationType() {
        helper.setProperty(KnownProperties.TEST_MIGRATE_TYPE, new MigrateDataType("1"));
        assertEquals(new MigrateDataType("1"),helper.getMigrationType(KnownProperties.TEST_MIGRATE_TYPE));
    }

    @Test
    public void getMigrationTypeList() {
        helper.setProperty(KnownProperties.TEST_MIGRATE_TYPE_LIST, Arrays.asList(new MigrateDataType("1"),new MigrateDataType("3")));
        assertEquals(Arrays.asList(new MigrateDataType("1"),new MigrateDataType("3")),helper.getMigrationTypeList(KnownProperties.TEST_MIGRATE_TYPE_LIST));
    }


    @Test
    public void get_nullPropertyName() {
        assertNull(helper.get(null, KnownProperties.PropertyType.STRING));
    }

    @Test
    public void get_nullPropertyType() {
        assertNull(helper.get(KnownProperties.TEST_STRING, null));
    }

    @Test
    public void get_MismatchType() {
        assertNull(helper.get(KnownProperties.TEST_STRING, KnownProperties.PropertyType.NUMBER));
    }

    @Test
    public void get_nullPropertyValue() {
        helper.setProperty(KnownProperties.TEST_NUMBER, null);
        assertNull(helper.get(KnownProperties.TEST_NUMBER, KnownProperties.PropertyType.NUMBER));
    }

    @Test
    public void get_invalidType_String() {
        // Any code that actually does this is broken, but we should handle it gracefully
        helper.setProperty(KnownProperties.TEST_STRING, "abcd");
        helper.getPropertyMap().put(KnownProperties.TEST_STRING, Integer.valueOf(1234));
        assertNull(helper.get(KnownProperties.TEST_STRING, KnownProperties.PropertyType.STRING));
    }

    @Test
    public void get_invalidType_StringList() {
        // Any code that actually does this is broken, but we should handle it gracefully
        helper.setProperty(KnownProperties.TEST_STRING_LIST, "a,b,c,d");
        helper.getPropertyMap().put(KnownProperties.TEST_STRING_LIST, Arrays.asList(1,2,3,4));
        assertNull(helper.get(KnownProperties.TEST_STRING_LIST, KnownProperties.PropertyType.STRING_LIST));
    }

    @Test
    public void get_invalidType_Number() {
        // Any code that actually does this is broken, but we should handle it gracefully
        helper.setProperty(KnownProperties.TEST_NUMBER, 1234);
        helper.getPropertyMap().put(KnownProperties.TEST_NUMBER, "1234");
        assertNull(helper.get(KnownProperties.TEST_NUMBER, KnownProperties.PropertyType.NUMBER));
    }

    @Test
    public void get_invalidType_NumberList() {
        // Any code that actually does this is broken, but we should handle it gracefully
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, "1,2,3,4");
        helper.getPropertyMap().put(KnownProperties.TEST_NUMBER_LIST, Arrays.asList("a","b","c","d"));
        assertNull(helper.get(KnownProperties.TEST_NUMBER_LIST, KnownProperties.PropertyType.NUMBER_LIST));
    }

    @Test
    public void get_invalidType_Boolean() {
        // Any code that actually does this is broken, but we should handle it gracefully
        helper.setProperty(KnownProperties.TEST_BOOLEAN, "true");
        helper.getPropertyMap().put(KnownProperties.TEST_BOOLEAN, Integer.valueOf(1234));
        assertNull(helper.get(KnownProperties.TEST_BOOLEAN, KnownProperties.PropertyType.BOOLEAN));
    }

    @Test
    public void get_invalidType_MigrateDataType() {
        // Any code that actually does this is broken, but we should handle it gracefully
        helper.setProperty(KnownProperties.TEST_MIGRATE_TYPE, "1");
        helper.getPropertyMap().put(KnownProperties.TEST_MIGRATE_TYPE, Integer.valueOf(1));
        assertNull(helper.get(KnownProperties.TEST_MIGRATE_TYPE, KnownProperties.PropertyType.MIGRATION_TYPE));
    }

    @Test
    public void get_invalidType_MigrateDataTypeList() {
        // Any code that actually does this is broken, but we should handle it gracefully
        helper.setProperty(KnownProperties.TEST_MIGRATE_TYPE_LIST, "1,1,2");
        helper.getPropertyMap().put(KnownProperties.TEST_MIGRATE_TYPE_LIST, Arrays.asList(1,1,2));
        assertNull(helper.get(KnownProperties.TEST_MIGRATE_TYPE_LIST, KnownProperties.PropertyType.MIGRATION_TYPE_LIST));
    }

    // TODO: breaks with error SLF4J: Class path contains multiple SLF4J bindings.
    //@Test
    public void loadSparkConf() {
        SparkConf sc = new SparkConf();
        sc.set(KnownProperties.TEST_STRING, "local");
        sc.set(KnownProperties.TEST_STRING_LIST, "A,B,C");
        sc.set(KnownProperties.TEST_NUMBER, "1");
        sc.set(KnownProperties.TEST_NUMBER_LIST, "4,5,6");
        sc.set(KnownProperties.TEST_BOOLEAN, "true");
        sc.set(KnownProperties.TEST_MIGRATE_TYPE, "1");
        sc.set(KnownProperties.TEST_MIGRATE_TYPE_LIST, "1,2");
        helper.initializeSparkConf(sc);
//        assertTrue(helper.isSparkConfFullyLoaded());
    }

}