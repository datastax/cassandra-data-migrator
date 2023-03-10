package datastax.astra.migrate.properties;

import java.util.List;
import java.util.Arrays;

import datastax.astra.migrate.MigrateDataType;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PropertyHelperTest {
    PropertyHelper helper;
    @Before
    public void setup() {
        helper = new PropertyHelper();
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
        helper.setSparkConf(sc);
        assertTrue(helper.isSparkConfFullyLoaded());
    }

}