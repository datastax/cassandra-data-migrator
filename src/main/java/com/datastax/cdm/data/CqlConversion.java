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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

public class CqlConversion {
    public static final Logger logger = LoggerFactory.getLogger(CqlConversion.class);
    public static final ProtocolVersion PROTOCOL_VERSION = ProtocolVersion.DEFAULT;

    enum Type {
        NONE, CODEC, UDT, LIST, SET, MAP,
        // TODO: add TUPLE to this list if we want to convert element types within a tuple
        UNSUPPORTED
    }

    private final List<DataType> fromDataTypeList;
    private final List<DataType> toDataTypeList;
    private final List<Type> conversionTypeList;
    private final CodecRegistry codecRegistry;

    public CqlConversion(DataType fromDataType, DataType toDataType, CodecRegistry codecRegistry) {
        if (null == fromDataType || null == toDataType || null == codecRegistry)
            throw new IllegalArgumentException(
                    "CqlConversion() - fromDataType, toDataType, and codecRegistry must be non-null");

        CqlData.Type fromCqlDataType = CqlData.toType(fromDataType);
        CqlData.Type toCqlDataType = CqlData.toType(toDataType);

        this.fromDataTypeList = new ArrayList<>();
        this.toDataTypeList = new ArrayList<>();
        this.conversionTypeList = new ArrayList<>();
        this.codecRegistry = codecRegistry;

        if (logger.isDebugEnabled())
            logger.debug("CqlConversion() - fromDataType: {}/{} toDataType: {}/{}", fromDataType, fromCqlDataType,
                    toDataType, toCqlDataType);

        if (fromCqlDataType == toCqlDataType && fromCqlDataType == CqlData.Type.PRIMITIVE) {
            fromDataTypeList.add(fromDataType);
            toDataTypeList.add(toDataType);
            this.conversionTypeList.add(calcConversionTypeForPrimitives(fromDataType, toDataType, codecRegistry));
        } else if (CqlData.isCollection(fromDataType) && CqlData.isCollection(toDataType)
                && CqlData.toType(fromDataType) == CqlData.toType(toDataType)) {
            fromDataTypeList.addAll(CqlData.extractDataTypesFromCollection(fromDataType));
            toDataTypeList.addAll(CqlData.extractDataTypesFromCollection(toDataType));
            conversionTypeList.addAll(calcConversionTypeForCollections(fromDataType, toDataType, codecRegistry));
        } else {
            logger.warn("Conversion does not currently know how to convert between {} and {}",
                    fromDataType.asCql(true, true), toDataType.asCql(true, true));
            fromDataTypeList.add(fromDataType);
            toDataTypeList.add(toDataType);
            conversionTypeList.add(Type.UNSUPPORTED);
        }
    }

    public Object convert(Object inputData) {
        if (null == conversionTypeList || conversionTypeList.isEmpty())
            return inputData;

        if (logger.isTraceEnabled())
            logger.trace("convert() - inputData: {}, converter: {}", inputData, this);

        // The first element on the conversionTypeList tells us what conversion the top-level object requires
        Type conversionType = conversionTypeList.get(0);
        switch (conversionType) {
        case NONE:
        case UNSUPPORTED:
            return inputData;
        case CODEC:
        case UDT:
            return convert_ONE(conversionType, inputData, fromDataTypeList.get(0), toDataTypeList.get(0),
                    codecRegistry);
        case LIST:
        case SET:
        case MAP:
            return convert_COLLECTION(conversionType, inputData,
                    conversionTypeList.subList(1, conversionTypeList.size()), fromDataTypeList, toDataTypeList,
                    codecRegistry);
        }
        logger.warn("Conversion.convert() - Unknown conversion type: {}", conversionType);
        return inputData;
    }

    public static List<CqlConversion> getConversions(CqlTable fromTable, CqlTable toTable) {
        if (null == fromTable || null == toTable)
            throw new IllegalArgumentException("fromTable and/or toTable is null");

        List<CqlConversion> cqlConversions = new ArrayList<>();
        List<DataType> fromDataTypes = fromTable.getColumnCqlTypes();
        List<DataType> toDataTypes = toTable.getColumnCqlTypes();

        if (logger.isDebugEnabled())
            logger.debug("getConversions() - From {} columns {} of types {}",
                    fromTable.isOrigin() ? "origin" : "target", fromTable.getColumnNames(false), fromDataTypes);
        if (logger.isDebugEnabled())
            logger.debug("getConversions() -   To {} columns {} of types {}", toTable.isOrigin() ? "origin" : "target",
                    toTable.getColumnNames(false), toDataTypes);

        for (int i = 0; i < fromDataTypes.size(); i++) {
            DataType fromDataType = fromDataTypes.get(i);
            if (null == fromDataType) {
                if (logger.isTraceEnabled())
                    logger.trace("At fromIndex {}, fromDataType is null, setting null conversion", i);
                cqlConversions.add(null);
                continue;
            }
            int correspondingIndex = fromTable.getCorrespondingIndex(i);
            if (correspondingIndex < 0 || correspondingIndex >= toDataTypes.size()) {
                if (logger.isTraceEnabled())
                    logger.trace("At fromIndex {}, correspondingIndex is {}, setting null conversion", i,
                            correspondingIndex);
                cqlConversions.add(null);
                continue;
            }
            DataType toDataType = toDataTypes.get(correspondingIndex);
            if (null == toDataType) {
                if (logger.isTraceEnabled())
                    logger.trace("At fromIndex {}, toDataType is null, setting null conversion", i);
                cqlConversions.add(null);
            } else {
                cqlConversions.add(new CqlConversion(fromDataType, toDataType, fromTable.getCodecRegistry()));
                if (logger.isTraceEnabled())
                    logger.trace("At fromIndex {} (correspondingIndex {}), have added {}", i, correspondingIndex,
                            cqlConversions.get(cqlConversions.size() - 1));
            }
        }

        return cqlConversions;
    }

    private static Type calcConversionTypeForPrimitives(DataType fromDataType, DataType toDataType,
            CodecRegistry codecRegistry) {
        if (CqlData.isPrimitive(fromDataType) && CqlData.isPrimitive(toDataType)) {
            if (fromDataType.equals(toDataType))
                return Type.NONE;
            else {
                TypeCodec<?> fromCodec = codecRegistry.codecFor(fromDataType);
                TypeCodec<?> toCodec = codecRegistry.codecFor(toDataType);
                if (toCodec.getJavaType().getRawType().isAssignableFrom(fromCodec.getJavaType().getRawType()))
                    return Type.NONE;
                else
                    return Type.CODEC;
            }
        }
        logger.warn("calcConversionTypeForPrimitives requires both types be primitive types: {} and {}",
                fromDataType.asCql(true, true), toDataType.asCql(true, true));
        return Type.UNSUPPORTED;
    }

    private static List<Type> calcConversionTypeForCollections(DataType fromDataType, DataType toDataType,
            CodecRegistry codecRegistry) {
        CqlData.Type fromType = CqlData.toType(fromDataType);
        CqlData.Type toType = CqlData.toType(toDataType);

        if (logger.isTraceEnabled())
            logger.trace("calcConversionTypeForCollections() - fromType: {}, toType: {}", fromType, toType);

        if (CqlData.isCollection(fromDataType) && fromType.equals(toType)) {
            // If the collection is a UDT, then we are done - no need to review elements as convert_UDT will handle it
            if (CqlData.Type.UDT.equals(fromType))
                return Collections.singletonList(Type.UDT);

            List<DataType> fromElementTypes = CqlData.extractDataTypesFromCollection(fromDataType);
            List<DataType> toElementTypes = CqlData.extractDataTypesFromCollection(toDataType);
            if (fromElementTypes.size() != toElementTypes.size()) {
                logger.warn("Collections must have same number of elements: {} and {}", fromDataType.asCql(true, true),
                        toDataType.asCql(true, true));
                return Collections.singletonList(Type.UNSUPPORTED);
            }

            // The first entry on the return list will be the conversion type for the collection itself
            // The rest will be the conversion types for the elements of the collection
            List<Type> rtn = new ArrayList<>();
            switch (fromType) {
            case LIST:
                rtn.add(Type.LIST);
                break;
            case SET:
                rtn.add(Type.SET);
                break;
            case MAP:
                rtn.add(Type.MAP);
                break;
            default:
                logger.warn("calcConversionTypeForCollections requires collection type to be LIST, SET, or MAP: {}",
                        fromDataType.asCql(true, true));
                return Collections.singletonList(Type.UNSUPPORTED);
            }

            for (int i = 0; i < fromElementTypes.size(); i++) {
                DataType fromElementType = fromElementTypes.get(i);
                DataType toElementType = toElementTypes.get(i);
                if (fromElementType.equals(toElementType)) {
                    rtn.add(Type.NONE);
                } else if (CqlData.isPrimitive(fromElementType) && CqlData.isPrimitive(toElementType)) {
                    rtn.add(calcConversionTypeForPrimitives(fromElementType, toElementType, codecRegistry));
                } else if (fromElementType instanceof UserDefinedType && toElementType instanceof UserDefinedType) {
                    rtn.add(Type.UDT);
                } else {
                    logger.warn("Within {}, do not know how to convert between element types {} and {}",
                            fromDataType.asCql(true, true), fromElementType.asCql(true, true),
                            toElementType.asCql(true, true));
                    rtn.add(Type.UNSUPPORTED);
                }
            }
            return rtn;
        }
        logger.warn("calcConversionTypeForCollections requires both types be collections of the same type: {} and {}",
                fromDataType.asCql(true, true), toDataType.asCql(true, true));
        return Collections.singletonList(Type.UNSUPPORTED);
    }

    protected static Object convert_ONE(Type conversionType, Object inputData, DataType fromDataType,
            DataType toDataType, CodecRegistry codecRegistry) {
        if (logger.isDebugEnabled())
            logger.debug("convert_ONE conversionType {} inputData {} fromDataType {} toDataType {}", conversionType,
                    inputData, fromDataType, toDataType);
        switch (conversionType) {
        case NONE:
        case UNSUPPORTED:
            return inputData;
        case CODEC:
            return convert_CODEC(inputData, fromDataType, toDataType, codecRegistry);
        case UDT:
            return convert_UDT((UdtValue) inputData, (UserDefinedType) fromDataType, (UserDefinedType) toDataType);
        }
        return inputData;
    }

    @SuppressWarnings("unchecked")
    protected static Object convert_CODEC(Object value, DataType fromDataType, DataType toDataType,
            CodecRegistry codecRegistry) {

        Class<?> fromClass = CqlData.getBindClass(fromDataType);
        Class<?> toClass = CqlData.getBindClass(toDataType);

        if (logger.isDebugEnabled())
            logger.debug("convert_CODEC value {} from {} to {}", value, fromClass, toClass);

        if (!fromClass.isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException(
                    "Value is not of type " + fromClass.getName() + " but of type " + value.getClass().getName());
        }

        TypeCodec<Object> fromCodec = (TypeCodec<Object>) codecRegistry.codecFor(toDataType, fromClass);
        if (fromCodec == null) {
            throw new IllegalArgumentException("No codec found in codecRegistry for Java type " + fromClass.getName()
                    + " to CQL type " + toDataType);
        }
        TypeCodec<Object> toCodec = (TypeCodec<Object>) codecRegistry.codecFor(toDataType, toClass);
        if (toCodec == null) {
            throw new IllegalArgumentException("No codec found in codecRegistry for Java type " + toClass.getName()
                    + " to CQL type " + toDataType);
        }
        ByteBuffer encoded = fromCodec.encode(value, PROTOCOL_VERSION);
        return toCodec.decode(encoded, PROTOCOL_VERSION);
    }

    protected static UdtValue convert_UDT(UdtValue fromUDTValue, UserDefinedType fromUDT, UserDefinedType toUDT) {
        if (logger.isDebugEnabled())
            logger.debug("convert_UDT fromUDTValue {} of class {} and type {}, converting fromUDT {} toUDT {}",
                    CqlData.getFormattedContent(CqlData.toType(fromUDT), fromUDTValue),
                    fromUDTValue.getClass().getName(), fromUDTValue.getType(), fromUDT, toUDT);
        if (null == fromUDTValue)
            return null;

        List<DataType> fromFieldTypes = fromUDT.getFieldTypes();
        List<DataType> toFieldTypes = toUDT.getFieldTypes();
        if (null == fromFieldTypes || null == toFieldTypes || fromFieldTypes.size() != toFieldTypes.size()) {
            throw new IllegalArgumentException("fromUDT and toUDT not be null and must have the same number of fields");
        }
        if (!fromUDTValue.getType().getClass().equals(fromUDT.getClass())) {
            throw new IllegalArgumentException("fromUDT and fromUDTValue must be of the same Java class");
        }

        UdtValue toUDTValue = toUDT.newValue();

        int loopLimit = fromFieldTypes.size();
        for (int i = 0; i < loopLimit; i++) {
            DataType fromFieldType = fromFieldTypes.get(i);
            TypeCodec<Object> fromCodec = fromUDT.getAttachmentPoint().getCodecRegistry().codecFor(fromFieldType);
            Object fromFieldValue = fromUDTValue.get(i, fromCodec);

            DataType toFieldType = toFieldTypes.get(i);
            TypeCodec<Object> toCodec = toUDT.getAttachmentPoint().getCodecRegistry().codecFor(toFieldType);
            Object toFieldValue = toCodec.parse(fromCodec.format(fromFieldValue));

            toUDTValue.set(i, toFieldValue, toCodec);
        }

        if (logger.isDebugEnabled())
            logger.debug("convert_UDT returning {} of type {}",
                    CqlData.getFormattedContent(CqlData.toType(toUDT), toUDTValue), toUDTValue.getType());
        return toUDTValue;
    }

    protected static Object convert_COLLECTION(Type collectionType, Object value, List<Type> conversionTypeList,
            List<DataType> fromDataTypeList, List<DataType> toDataTypeList, CodecRegistry codecRegistry) {
        CqlData.Type firstDataType = (null == fromDataTypeList || fromDataTypeList.isEmpty()) ? CqlData.Type.UNKNOWN
                : CqlData.toType(fromDataTypeList.get(0));
        if (logger.isDebugEnabled())
            logger.debug(
                    "convert_COLLECTION collectionType {} value {} conversionTypeList {} fromDataTypeList {} toDataTypeList {}",
                    collectionType, CqlData.getFormattedContent(firstDataType, value), conversionTypeList,
                    fromDataTypeList, toDataTypeList);
        if (null == value) {
            return null;
        }
        if (null == collectionType || null == conversionTypeList || null == fromDataTypeList || null == toDataTypeList
                || conversionTypeList.isEmpty() || fromDataTypeList.isEmpty() || toDataTypeList.isEmpty()) {
            throw new IllegalArgumentException(
                    "conversionType, conversionTypeList, fromDataTypeList, and toDataTypeList must not be null and must not be empty");
        }
        if (conversionTypeList.size() != fromDataTypeList.size()
                || conversionTypeList.size() != toDataTypeList.size()) {
            throw new IllegalArgumentException(
                    "conversionTypeList, fromDataTypeList, and toDataTypeList must be the same size");
        }
        if (null == codecRegistry) {
            throw new IllegalArgumentException("codecRegistry must not be null");
        }

        // If all elements in conversionTypeList are either NONE or UNKNOWN, then return the original value
        if (conversionTypeList.stream().allMatch(t -> Type.NONE.equals(t) || Type.UNSUPPORTED.equals(t)))
            return value;

        switch (collectionType) {
        case LIST:
            return ((List<?>) value).stream().map(v -> convert_ONE(conversionTypeList.get(0), v,
                    fromDataTypeList.get(0), toDataTypeList.get(0), codecRegistry)).collect(Collectors.toList());
        case SET:
            return ((Set<?>) value).stream().map(v -> convert_ONE(conversionTypeList.get(0), v, fromDataTypeList.get(0),
                    toDataTypeList.get(0), codecRegistry)).collect(Collectors.toSet());
        case MAP:
            // There are two conversion types in the element list: one for keys and one for values
            return ((Map<?, ?>) value).entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> convert_ONE(conversionTypeList.get(0), entry.getKey(), fromDataTypeList.get(0),
                                    toDataTypeList.get(0), codecRegistry),
                            entry -> convert_ONE(conversionTypeList.get(1), entry.getValue(), fromDataTypeList.get(1),
                                    toDataTypeList.get(1), codecRegistry)));
        }
        return value;
    }

    List<Type> getConversionTypeList() {
        return conversionTypeList;
    }

    List<DataType> getFromDataTypeList() {
        return fromDataTypeList;
    }

    List<DataType> getToDataTypeList() {
        return toDataTypeList;
    }

    @Override
    public String toString() {
        return "CqlData{" + "fromDataTypeList=" + fromDataTypeList + ", toDataTypeList=" + toDataTypeList
                + ", conversionTypeList=" + conversionTypeList + '}';
    }

}
