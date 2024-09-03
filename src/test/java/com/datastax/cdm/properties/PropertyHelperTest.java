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
package com.datastax.cdm.properties;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PropertyHelperTest {
    PropertyHelper helper;
    SparkConf validSparkConf;

    @BeforeEach
    public void setup() {
        helper = PropertyHelper.getInstance();
    }

    @AfterEach
    public void tearDown() {
        PropertyHelper.destroyInstance();
        validSparkConf = null;
    }

    @Test
    public void setProperty_String() {
        String value = "test_value";
        String setValue = (String) helper.setProperty(KnownProperties.TEST_STRING, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_StringList() {
        List<String> value = Arrays.asList("a", "b", "c");
        List<String> setValue = (List<String>) helper.setProperty(KnownProperties.TEST_STRING_LIST, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_StringList_oneValue() {
        List<String> value = Arrays.asList("a");
        List<String> setValue = (List<String>) helper.setProperty(KnownProperties.TEST_STRING_LIST, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_StringList_splitString() {
        String list = "a,b,c";
        List<String> setValue = (List<String>) helper.setProperty(KnownProperties.TEST_STRING_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.STRING_LIST, list));
        assertEquals(Arrays.asList(list.split(",")), setValue);
    }

    @Test
    public void setProperty_StringList_splitString_oneValue() {
        String list = "a";
        List<String> setValue = (List<String>) helper.setProperty(KnownProperties.TEST_STRING_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.STRING_LIST, list));
        assertEquals(Arrays.asList(list), setValue);
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
        List<Integer> value = Arrays.asList(1, 2, 3, 4);
        List<Integer> setValue = (List<Integer>) helper.setProperty(KnownProperties.TEST_NUMBER_LIST, value);
        assertEquals(value, setValue);
    }

    @Test
    public void setProperty_NumberList_splitString() {
        String list = "1,2,3,4";
        List<Long> setValue = (List<Long>) helper.setProperty(KnownProperties.TEST_NUMBER_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.NUMBER_LIST, list));
        assertEquals(Arrays.asList(1L, 2L, 3L, 4L), setValue);
    }

    @Test
    public void setProperty_NumberList_splitString_oneValue() {
        String list = "1";
        List<Long> setValue = (List<Long>) helper.setProperty(KnownProperties.TEST_NUMBER_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.NUMBER_LIST, list));
        assertEquals(Arrays.asList(1L), setValue);
    }

    @Test
    public void setProperty_NumberList_splitString_LongValue() {
        String list = String.valueOf(Long.MAX_VALUE);
        List<Long> setValue = (List<Long>) helper.setProperty(KnownProperties.TEST_NUMBER_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.NUMBER_LIST, list));
        assertEquals(Arrays.asList(Long.MAX_VALUE), setValue);
    }

    @Test
    public void setProperty_NumberList_splitString_badNumber() {
        String list = "1,2,x,4";
        List<Integer> setValue = (List<Integer>) helper.setProperty(KnownProperties.TEST_NUMBER_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.NUMBER_LIST, list));
        assertNull(setValue);
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
        assertNull(helper.setProperty(KnownProperties.TEST_UNHANDLED_TYPE, "abc"));
    }

    @Test
    public void setProperty_unknownValue() {
        assertNull(helper.setProperty("unknown.value", "abc"));
    }

    @Test
    public void get_null() {
        assertNull(helper.get(null));
    }

    @Test
    public void getString() {
        helper.setProperty(KnownProperties.TEST_STRING, "test");
        assertEquals("test", helper.getString(KnownProperties.TEST_STRING));
    }

    @Test
    public void getStringList() {
        helper.setProperty(KnownProperties.TEST_STRING_LIST, Arrays.asList("a", "b", "c"));
        assertEquals(Arrays.asList("a", "b", "c"), helper.getStringList(KnownProperties.TEST_STRING_LIST));
    }

    @Test
    public void getNumber() {
        helper.setProperty(KnownProperties.TEST_NUMBER, 4321);
        assertEquals(4321, helper.getNumber(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getNumber_NullValue() {
        helper.setProperty(KnownProperties.TEST_NUMBER, null);
        assertNull(helper.getNumber(KnownProperties.TEST_NUMBER));
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
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1, 2, 3));
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
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1, 2, 3));
        assertEquals(Arrays.asList(1, 2, 3), helper.getNumberList(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getIntegerList() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1, 2, 3));
        assertEquals(Arrays.asList(1, 2, 3), helper.getIntegerList(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getIntegerList_nullParameter() {
        assertNull(helper.getIntegerList(null));
    }

    @Test
    public void getIntegerList_nullValue() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1, null, 3));
        assertNull(helper.getIntegerList(null));
    }

    @Test
    public void getIntegerList_wrongType() {
        helper.setProperty(KnownProperties.TEST_STRING_LIST, Arrays.asList("1", "2", "3"));
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
        helper.getPropertyMap().put(KnownProperties.TEST_STRING_LIST, Arrays.asList(1, 2, 3, 4));
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
        helper.getPropertyMap().put(KnownProperties.TEST_NUMBER_LIST, Arrays.asList("a", "b", "c", "d"));
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
    public void getAsString_String() {
        helper.setProperty(KnownProperties.TEST_STRING, "abcd");
        assertEquals("abcd", helper.getAsString(KnownProperties.TEST_STRING));
    }

    @Test
    public void getAsString_StringList() {
        helper.setProperty(KnownProperties.TEST_STRING_LIST, Arrays.asList("a", "b", "c", "d"));
        assertEquals("a,b,c,d", helper.getAsString(KnownProperties.TEST_STRING_LIST));
    }

    @Test
    public void getAsString_Number() {
        helper.setProperty(KnownProperties.TEST_NUMBER, 1234);
        assertEquals("1234", helper.getAsString(KnownProperties.TEST_NUMBER));
    }

    @Test
    public void getAsString_NumberList() {
        helper.setProperty(KnownProperties.TEST_NUMBER_LIST, Arrays.asList(1, 2, 3, 4));
        assertEquals("1,2,3,4", helper.getAsString(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getAsString_Boolean() {
        helper.setProperty(KnownProperties.TEST_BOOLEAN, true);
        assertEquals("true", helper.getAsString(KnownProperties.TEST_BOOLEAN));
    }

    @Test
    public void getAsString_valueNotSet_string() {
        assertEquals("", helper.getAsString(KnownProperties.TEST_STRING_NO_DEFAULT));
    }

    @Test
    public void getAsString_nullArgument() {
        assertNull(helper.getAsString(null));
    }

    @Test
    public void getAsString_nullUnhanldedType() {
        helper.setProperty(KnownProperties.TEST_UNHANDLED_TYPE, "abcd");
        assertEquals("", helper.getAsString(KnownProperties.TEST_UNHANDLED_TYPE));
    }

    @Test
    public void getInstance() {
        SparkConf sc = new SparkConf();
        PropertyHelper helper = PropertyHelper.getInstance(sc);
        assertNotNull(helper);
    }

    @Test
    public void initializeSparkConf_null() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> {
            helper.initializeSparkConf(null);
        });
        assertTrue(e.getMessage().contains("SparkConf cannot be null"));
    }

    @Test
    public void loadSparkConf_missingRequired() {
        SparkConf sc = new SparkConf();
        helper.initializeSparkConf(sc);
        assertFalse(helper.isSparkConfFullyLoaded());
    }

    @Test
    public void loadSparkConf_withRequired() {
        setValidSparkConf();
        helper.initializeSparkConf(validSparkConf);
        assertTrue(helper.isSparkConfFullyLoaded());
    }

    @Test
    public void loadSparkConf_withRequiredAndAllTypes() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.TEST_STRING, "local");
        validSparkConf.set(KnownProperties.TEST_STRING_LIST, "A,B,C");
        validSparkConf.set(KnownProperties.TEST_NUMBER, "1");
        validSparkConf.set(KnownProperties.TEST_NUMBER_LIST, "4,5,6");
        validSparkConf.set(KnownProperties.TEST_BOOLEAN, "true");
        helper.initializeSparkConf(validSparkConf);
        assertTrue(helper.isSparkConfFullyLoaded());
    }

    @Test
    public void loadSparkConf_badValue() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.TEST_NUMBER_LIST, "a,b,c");
        helper.initializeSparkConf(validSparkConf);
        assertFalse(helper.isSparkConfFullyLoaded());
    }

    @Test
    public void loadSparkConf_incompleteSourceTLS() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.ORIGIN_TLS_ENABLED, "true");
        helper.initializeSparkConf(validSparkConf);
        assertFalse(helper.isSparkConfFullyLoaded());
    }

    @Test
    public void loadSparkConf_incompleteTargetTLS() {
        setValidSparkConf();
        validSparkConf.set(KnownProperties.TARGET_TLS_ENABLED, "true");
        helper.initializeSparkConf(validSparkConf);
        assertFalse(helper.isSparkConfFullyLoaded());
    }

    @Test
    public void test_invalid_origin_connection() {
        validSparkConf = new SparkConf();
        validSparkConf.set(KnownProperties.CONNECT_ORIGIN_HOST, "");
        validSparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "ks.tab1");
        validSparkConf.set(KnownProperties.CONNECT_TARGET_HOST, "localhost");
        validSparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "ks.tab1");
        helper.initializeSparkConf(validSparkConf);
        assertFalse(helper.isValidConfig());
    }

    @Test
    public void test_invalid_target_connection() {
        validSparkConf = new SparkConf();
        validSparkConf.set(KnownProperties.CONNECT_ORIGIN_HOST, "localhost");
        validSparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "ks.tab1");
        validSparkConf.set(KnownProperties.CONNECT_TARGET_HOST, "");
        validSparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "ks.tab1");
        helper.initializeSparkConf(validSparkConf);
        assertFalse(helper.isValidConfig());
    }

    @Test
    public void test_valid_connection_noUserPassword() {
        validSparkConf = new SparkConf();
        validSparkConf.set(KnownProperties.CONNECT_ORIGIN_HOST, "localhost");
        validSparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "ks.tab1");
        validSparkConf.set(KnownProperties.CONNECT_TARGET_HOST, "localhost");
        validSparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "ks.tab1");
        validSparkConf.set(KnownProperties.CONNECT_ORIGIN_USERNAME, "");
        validSparkConf.set(KnownProperties.CONNECT_ORIGIN_PASSWORD, "");
        validSparkConf.set(KnownProperties.CONNECT_TARGET_USERNAME, "");
        validSparkConf.set(KnownProperties.CONNECT_TARGET_PASSWORD, "");
        helper.initializeSparkConf(validSparkConf);
        assertTrue(helper.isValidConfig());
    }

    @Test
    public void meetsMinimum_true() {
        assertTrue(helper.meetsMinimum("a", 100L, 0L));
    }

    @Test
    public void meetsMinimum_false() {
        assertFalse(helper.meetsMinimum("a", 1L, 100L));
    }

    private void setValidSparkConf() {
        validSparkConf = new SparkConf();
        validSparkConf.set(KnownProperties.CONNECT_ORIGIN_HOST, "localhost");
        validSparkConf.set(KnownProperties.ORIGIN_KEYSPACE_TABLE, "ks.tab1");
        validSparkConf.set(KnownProperties.CONNECT_TARGET_HOST, "localhost");
        validSparkConf.set(KnownProperties.TARGET_KEYSPACE_TABLE, "ks.tab1");
    }

}
