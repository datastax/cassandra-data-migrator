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
package com.datastax.cdm.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.VectorType;

public class CqlData {
    public enum Type {
        PRIMITIVE, UDT, LIST, SET, MAP, TUPLE, VECTOR, UNKNOWN
    }

    private static final Map<DataType, Class<?>> primitiveDataTypeToJavaClassMap = new HashMap<>();
    static {
        primitiveDataTypeToJavaClassMap.put(DataTypes.TEXT, String.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.ASCII, String.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.INT, Integer.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.BIGINT, Long.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.DOUBLE, Double.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.FLOAT, Float.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.BOOLEAN, Boolean.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.UUID, java.util.UUID.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.TIMESTAMP, java.time.Instant.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.DATE, java.time.LocalDate.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.TIME, java.time.LocalTime.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.INET, java.net.InetAddress.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.SMALLINT, Short.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.TINYINT, Byte.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.BLOB, java.nio.ByteBuffer.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.VARINT, java.math.BigInteger.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.DECIMAL, java.math.BigDecimal.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.TIMEUUID, java.util.UUID.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.COUNTER, Long.class);
        primitiveDataTypeToJavaClassMap.put(DataTypes.DURATION,
                com.datastax.oss.driver.api.core.data.CqlDuration.class);
        primitiveDataTypeToJavaClassMap.put(DseDataTypes.POLYGON,
                com.datastax.dse.driver.api.core.data.geometry.Polygon.class);
        primitiveDataTypeToJavaClassMap.put(DseDataTypes.POINT,
                com.datastax.dse.driver.api.core.data.geometry.Point.class);
        primitiveDataTypeToJavaClassMap.put(DseDataTypes.LINE_STRING,
                com.datastax.dse.driver.api.core.data.geometry.LineString.class);
        primitiveDataTypeToJavaClassMap.put(DseDataTypes.DATE_RANGE,
                com.datastax.dse.driver.api.core.data.time.DateRange.class);
    }

    public static Type toType(DataType dataType) {
        if (isPrimitive(dataType))
            return Type.PRIMITIVE;
        if (dataType instanceof ListType)
            return Type.LIST;
        if (dataType instanceof SetType)
            return Type.SET;
        if (dataType instanceof MapType)
            return Type.MAP;
        if (dataType instanceof TupleType)
            return Type.TUPLE;
        if (dataType instanceof UserDefinedType)
            return Type.UDT;
        if (dataType instanceof VectorType)
            return Type.VECTOR;
        throw new RuntimeException("Unsupported data type: " + dataType);
    }

    public static boolean isPrimitive(DataType dataType) {
        return primitiveDataTypeToJavaClassMap.containsKey(dataType);
    }

    public static boolean isCollection(DataType dataType) {
        if (dataType instanceof UserDefinedType)
            return true;
        if (dataType instanceof ListType)
            return true;
        if (dataType instanceof SetType)
            return true;
        if (dataType instanceof MapType)
            return true;
        if (dataType instanceof TupleType)
            return true;
        if (dataType instanceof VectorType)
            return true;
        return false;
    }

    public static boolean isFrozen(DataType dataType) {
        if (isPrimitive(dataType))
            return false;
        if (dataType instanceof UserDefinedType)
            return ((UserDefinedType) dataType).isFrozen();
        if (dataType instanceof ListType)
            return ((ListType) dataType).isFrozen();
        if (dataType instanceof SetType)
            return ((SetType) dataType).isFrozen();
        if (dataType instanceof MapType)
            return ((MapType) dataType).isFrozen();
        if (dataType instanceof TupleType)
            return dataType.asCql(true, false).toLowerCase().contains("frozen<");
        // vector CQL data type doesn't support frozen
        return false;
    }

    public static Class getBindClass(DataType dataType) {
        Class primitiveClass = primitiveDataTypeToJavaClassMap.get(dataType);
        if (primitiveClass != null)
            return primitiveClass;
        if (dataType instanceof ListType)
            return java.util.List.class;
        if (dataType instanceof SetType)
            return java.util.Set.class;
        if (dataType instanceof MapType)
            return java.util.Map.class;
        if (dataType instanceof UserDefinedType)
            return com.datastax.oss.driver.api.core.data.UdtValue.class;
        if (dataType instanceof TupleType)
            return com.datastax.oss.driver.api.core.data.TupleValue.class;
        if (dataType instanceof VectorType)
            return com.datastax.oss.driver.api.core.data.CqlVector.class;

        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    public static List<DataType> extractDataTypesFromCollection(DataType dataType) {
        CqlData.Type type = CqlData.toType(dataType);
        switch (type) {
        case UDT:
            return Collections.singletonList(dataType);
        case LIST:
            return Collections.singletonList(((ListType) dataType).getElementType());
        case SET:
            return Collections.singletonList(((SetType) dataType).getElementType());
        case MAP:
            return Arrays.asList(((MapType) dataType).getKeyType(), ((MapType) dataType).getValueType());
        case TUPLE:
            return ((TupleType) dataType).getComponentTypes();
        case VECTOR:
            return Collections.singletonList(((VectorType) dataType).getElementType());
        default:
            return null;
        }
    }

    public static String getFormattedContent(Type type, Object value) {
        if (null == value) {
            return "";
        }
        String openBracket;
        String closeBracket;
        try {
            switch (type) {
            case UDT:
                return ((UdtValue) value).getFormattedContents();
            case LIST:
            case VECTOR:
                openBracket = "[";
                closeBracket = "]";
                break;
            case SET:
            case MAP:
                openBracket = "{";
                closeBracket = "}";
                break;
            case PRIMITIVE:
            case UNKNOWN:
            case TUPLE:
            default:
                return value.toString();
            }

            List<Object> objects = DataUtility.extractObjectsFromCollection(value);
            StringBuilder sb = new StringBuilder(openBracket);
            for (Object obj : objects) {
                if (obj instanceof UdtValue)
                    sb.append(((UdtValue) obj).getFormattedContents());
                else if (obj instanceof Map.Entry) {
                    Object mapKey = ((Map.Entry<?, ?>) obj).getKey();
                    Object mapValue = ((Map.Entry<?, ?>) obj).getValue();
                    String mapKeyStr = mapKey instanceof UdtValue ? ((UdtValue) mapKey).getFormattedContents()
                            : mapKey.toString();
                    String mapValueStr = mapValue instanceof UdtValue ? ((UdtValue) mapValue).getFormattedContents()
                            : mapValue.toString();
                    sb.append(mapKeyStr).append("=").append(mapValueStr);
                } else {
                    sb.append(obj.toString());
                }
                if (objects.indexOf(obj) < objects.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(closeBracket);
            return sb.toString();
        } catch (Exception e) {
            return value.toString();
        }
    }

}
