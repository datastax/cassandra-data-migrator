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

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KnownPropertiesTest {

    @Test
    public void getDefault_knownDefault() {
        assertEquals("text", KnownProperties.getDefaultAsString(KnownProperties.TEST_STRING));
    }

    @Test
    public void getDefault_knownNoDefault() {
        assertNull(KnownProperties.getDefaultAsString(KnownProperties.TEST_STRING_NO_DEFAULT));
    }

    @Test
    public void getDefault_unknownValue() {
        assertNull(KnownProperties.getDefaultAsString("unknown"));
    }

    @Test
    public void getDefault_nullKey() {
        assertNull(KnownProperties.getDefaultAsString(null));
    }

    @Test
    public void getType_knownType() {
        assertEquals(KnownProperties.PropertyType.STRING, KnownProperties.getType(KnownProperties.TEST_STRING));
    }

    @Test
    public void getType_unknownValue() {
        assertNull(KnownProperties.getType("unknown"));
    }

    @Test
    public void getType_nullKey() {
        assertNull(KnownProperties.getType(null));
    }

    @Test
    public void hasDefault_known() {
        assertTrue(KnownProperties.isKnown(KnownProperties.TEST_STRING));
    }

    @Test
    public void hasDefault_unknown() {
        assertFalse(KnownProperties.isKnown("unknown"));
    }

    @Test
    public void asType_String() {
        String value = "test";
        assertEquals(value, KnownProperties.asType(KnownProperties.PropertyType.STRING, value));
    }

    @Test
    public void asType_StringList() {
        String value = "a,b,c";
        assertEquals(Arrays.asList(value.split(",")),
                KnownProperties.asType(KnownProperties.PropertyType.STRING_LIST, value));
    }

    @Test
    public void asType_Number() {
        Long value = Long.MAX_VALUE;
        assertEquals(value, KnownProperties.asType(KnownProperties.PropertyType.NUMBER, String.valueOf(value)));
    }

    @Test
    public void asType_Number_Invalid() {
        assertNull(KnownProperties.asType(KnownProperties.PropertyType.NUMBER, "x"));
    }

    @Test
    public void asType_NumberList() {
        assertEquals(Arrays.asList(1L, 2L, 3L),
                KnownProperties.asType(KnownProperties.PropertyType.NUMBER_LIST, "1,2,3"));
    }

    @Test
    public void asType_NumberList_Invalid() {
        assertNull(KnownProperties.asType(KnownProperties.PropertyType.NUMBER_LIST, "1,2,x,4"));
    }

    @Test
    public void asType_Boolean() {
        assertEquals(true, KnownProperties.asType(KnownProperties.PropertyType.BOOLEAN, "true"));
    }

    @Test
    public void asType_unhandledType() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            KnownProperties.asType(KnownProperties.PropertyType.TEST_UNHANDLED_TYPE, "1");
        });
        assertTrue(exception.getMessage().contains("Unhandled property type: TEST_UNHANDLED_TYPE"));
    }

    @Test
    public void getDefaultAsString_NumberList() {
        Assertions.assertEquals(KnownProperties.TEST_NUMBER_LIST_DEFAULT,
                KnownProperties.getDefaultAsString(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getDefault_NumberList() {
        assertEquals(Arrays.asList(1L, 2L), KnownProperties.getDefault(KnownProperties.TEST_NUMBER_LIST));
    }

    @Test
    public void getDefault_nullArgument() {
        assertNull(KnownProperties.getDefault(null));
    }

    @Test
    public void getDefault_noDefault() {
        assertNull(KnownProperties.getDefault(KnownProperties.TEST_STRING_NO_DEFAULT));
    }

    @Test
    public void getTypeMap() {
        assertNotNull(KnownProperties.getTypeMap());
        assertEquals(KnownProperties.PropertyType.STRING,
                KnownProperties.getTypeMap().get(KnownProperties.TEST_STRING));
    }

    @Test
    public void getRequired() {
        assertNotNull(KnownProperties.getRequired());
        assertTrue(!KnownProperties.getRequired().isEmpty());
    }

    @Test
    public void validateType_String() {
        assertTrue(KnownProperties.validateType(KnownProperties.PropertyType.STRING, "test"));
    }

    @Test
    public void validateType_String_notString() {
        assertFalse(KnownProperties.validateType(KnownProperties.PropertyType.STRING, 1L));
    }

    @Test
    public void validateType_StringList() {
        assertTrue(KnownProperties.validateType(KnownProperties.PropertyType.STRING_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.STRING_LIST, "a,b,c")));
    }

    @Test
    public void validateType_StringList_Empty() {
        assertFalse(KnownProperties.validateType(KnownProperties.PropertyType.STRING_LIST, new ArrayList<String>()));
    }

    @Test
    public void validateType_StringList_MixedList() {
        ArrayList<Object> list = new ArrayList<>(2);
        list.add("a");
        list.add(1L);
        assertFalse(KnownProperties.validateType(KnownProperties.PropertyType.STRING_LIST, list));
    }

    @Test
    public void validateType_Number() {
        assertTrue(KnownProperties.validateType(KnownProperties.PropertyType.NUMBER,
                KnownProperties.asType(KnownProperties.PropertyType.NUMBER, "1")));
    }

    @Test
    public void validateType_NumberList() {
        assertTrue(KnownProperties.validateType(KnownProperties.PropertyType.NUMBER_LIST,
                KnownProperties.asType(KnownProperties.PropertyType.NUMBER_LIST, "1,2,3")));
    }

    @Test
    public void validateType_NumberList_Empty() {
        assertFalse(KnownProperties.validateType(KnownProperties.PropertyType.NUMBER_LIST, new ArrayList<Number>()));
    }

    @Test
    public void validateType_NumberList_MixedList() {
        ArrayList<Object> list = new ArrayList<>(2);
        list.add(1L);
        list.add("a");
        assertFalse(KnownProperties.validateType(KnownProperties.PropertyType.NUMBER_LIST, list));
    }

    @Test
    public void validateType_Boolean() {
        assertTrue(KnownProperties.validateType(KnownProperties.PropertyType.BOOLEAN,
                KnownProperties.asType(KnownProperties.PropertyType.BOOLEAN, "false")));
    }

}
