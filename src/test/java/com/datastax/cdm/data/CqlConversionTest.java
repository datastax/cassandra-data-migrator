package com.datastax.cdm.data;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CqlConversionTest {

    private DataType fromDataType;
    private DataType toDataType;
    private CodecRegistry codecRegistry;

    @BeforeEach
    void setUp() {
        fromDataType = mock(DataType.class);
        toDataType = mock(DataType.class);
        codecRegistry = mock(CodecRegistry.class);
    }

    @Test
    void testConstructorThrowsIllegalArgumentExceptionWhenArgumentsAreNull() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> new CqlConversion(null, toDataType, codecRegistry), "null fromDataType"),
                () -> assertThrows(IllegalArgumentException.class, () -> new CqlConversion(fromDataType, null, codecRegistry), "null toDataType"),
                () -> assertThrows(IllegalArgumentException.class, () -> new CqlConversion(fromDataType, toDataType, null), "null codecRegistry")
        );
    }

//    @Test
//    void testConvertWhenConversionTypeIsNone() {
//        CqlConversion.Type conversionType = CqlConversion.Type.NONE;
//        List<CqlConversion.Type> conversionTypeList = Collections.singletonList(conversionType);
//
//        CqlConversion cqlConversion = spy(new CqlConversion(fromDataType, toDataType, codecRegistry));
//        doReturn(conversionTypeList).when(cqlConversion).getConversionTypeList();
//
//        Object inputData = new Object();
//        Object result = cqlConversion.convert(inputData);
//
//        assertSame(inputData, result);
//    }
//
//    @Test
//    void testConvertWhenConversionTypeIsUnsupported() {
//        CqlConversion.Type conversionType = CqlConversion.Type.UNSUPPORTED;
//        List<CqlConversion.Type> conversionTypeList = Collections.singletonList(conversionType);
//
//        CqlConversion cqlConversion = spy(new CqlConversion(fromDataType, toDataType, codecRegistry));
//        doReturn(conversionTypeList).when(cqlConversion).getConversionTypeList();
//
//        Object inputData = new Object();
//        Object result = cqlConversion.convert(inputData);
//
//        assertSame(inputData, result);
//    }
//
//    @Test
//    void testConvertWhenConversionTypeIsCodec() {
//        CqlConversion.Type conversionType = CqlConversion.Type.CODEC;
//        List<CqlConversion.Type> conversionTypeList = Collections.singletonList(conversionType);
//
//        CqlConversion cqlConversion = spy(new CqlConversion(fromDataType, toDataType, codecRegistry));
//        doReturn(conversionTypeList).when(cqlConversion).getConversionTypeList();
//        doReturn(Collections.singletonList(fromDataType)).when(cqlConversion).getFromDataTypeList();
//        doReturn(Collections.singletonList(toDataType)).when(cqlConversion).getToDataTypeList();
//
//        Object inputData = new Object();
//        Object expectedResult = new Object();
//
//        // Stub the convert_ONE() method to return expectedResult when called with specific arguments
//        doReturn(expectedResult).when(cqlConversion).convert_ONE(conversionType, inputData, fromDataType, toDataType, codecRegistry);
//
//        Object result = cqlConversion.convert(inputData);
//
//        // Verify that convert_ONE() was called with the expected arguments
//        verify(cqlConversion).convert_ONE(conversionType, inputData, fromDataType, toDataType, codecRegistry);
//
//        assertEquals(expectedResult, result);
//    }

}
