package datastax.cdm.feature;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.*;

import static org.mockito.Mockito.*;

import static org.junit.jupiter.api.Assertions.*;

public class UDTMapperTest {

    private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

    private final UserDefinedType originUDT = new UserDefinedTypeBuilder(CqlIdentifier.fromCql("\"origin-ks\""), CqlIdentifier.fromCql("\"origin-udt\""))
            .withField(CqlIdentifier.fromCql("\"text-value\""), DataTypes.TEXT)
            .withField(CqlIdentifier.fromCql("\"long-value\""), DataTypes.BIGINT)
            .withField(CqlIdentifier.fromCql("\"double-value\""), DataTypes.DOUBLE)
            .build();

    private final UserDefinedType targetUDT = new UserDefinedTypeBuilder(CqlIdentifier.fromCql("target_ks"), CqlIdentifier.fromCql("target_udt"))
            .withField(CqlIdentifier.fromCql("text_value"), DataTypes.TEXT)
            .withField(CqlIdentifier.fromCql("long_value"), DataTypes.BIGINT)
            .withField(CqlIdentifier.fromCql("double_value"), DataTypes.DOUBLE)
            .build();

    private UdtValue createMockOriginUdtValue(String textValue, Long longValue, Double doubleValue) {
        UdtValue originUdtValue = mock(UdtValue.class);
        when(originUdtValue.get(0, codecRegistry.codecFor(DataTypes.TEXT))).thenReturn(textValue);
        when(originUdtValue.get(1, codecRegistry.codecFor(DataTypes.BIGINT))).thenReturn(longValue);
        when(originUdtValue.get(2, codecRegistry.codecFor(DataTypes.DOUBLE))).thenReturn(doubleValue);
        when(originUdtValue.getType()).thenReturn(originUDT);
        return originUdtValue;
    }

    @Test
    public void testMap() {
        UdtValue originUdtValue = createMockOriginUdtValue("text value", 42L, 3.1416);
        UdtValue targetUdtValue =  UDTMapper.convertUDTValue(originUDT, originUdtValue, targetUDT);
        assertUdtValueEquals(targetUdtValue);
    }

    @Test
    public void testConvertUdtValue() {
        UdtValue originUdtValue = createMockOriginUdtValue("text value", 42L, 3.1416);
        UdtValue convertedValue = (UdtValue) UDTMapper.convert(originUdtValue, new Tuple2<>(originUDT, targetUDT));
        assertUdtValueEquals(convertedValue);
    }

    @Test
    public void testConvertList() {
        UdtValue originUdtValue = createMockOriginUdtValue("text value", 42L, 3.1416);
        List<UdtValue> udtList = Arrays.asList(originUdtValue, originUdtValue);
        List<UdtValue> convertedList = (List<UdtValue>) UDTMapper.convert(udtList, new Tuple2<>(originUDT, targetUDT));
        assertEquals(2, convertedList.size());
        convertedList.forEach(this::assertUdtValueEquals);
    }

    @Test
    public void testConvertSet() {
        UdtValue originUdtValue = createMockOriginUdtValue("text value", 42L, 3.1416);
        Set<UdtValue> udtSet = new HashSet<>(Arrays.asList(originUdtValue, originUdtValue));
        Set<UdtValue> convertedSet = (Set<UdtValue>) UDTMapper.convert(udtSet, new Tuple2<>(originUDT, targetUDT));
        assertEquals(1, convertedSet.size());
        convertedSet.forEach(this::assertUdtValueEquals);
    }

    @Test
    public void testConvertMapWithUdtValueValues() {
        UdtValue originUdtValue1 = createMockOriginUdtValue("text value 1", 42L, 3.1416);
        UdtValue originUdtValue2 = createMockOriginUdtValue("text value 2", 43L, 4.1416);
        Map<String, UdtValue> udtMap = new HashMap<>();
        udtMap.put("key1", originUdtValue1);
        udtMap.put("key2", originUdtValue2);

        Map<String, UdtValue> convertedMap = (Map<String, UdtValue>) UDTMapper.convert(udtMap, new Tuple2<>(originUDT, targetUDT));
        assertEquals(2, convertedMap.size());
        convertedMap.forEach((key, value) -> {
            assertTrue(key.equals("key1") || key.equals("key2"));
            assertAll(
                    () -> assertEquals(key.equals("key1") ? "text value 1" : "text value 2", value.get(CqlIdentifier.fromCql("text_value"), codecRegistry.codecFor(DataTypes.TEXT)), "text value"),
                    () -> assertEquals(key.equals("key1") ? 42L : 43L, (Long) value.get(CqlIdentifier.fromCql("long_value"), codecRegistry.codecFor(DataTypes.BIGINT)), "long value"),
                    () -> assertEquals(key.equals("key1") ? 3.1416 : 4.1416, value.get(CqlIdentifier.fromCql("double_value"), codecRegistry.codecFor(DataTypes.DOUBLE)), "double value")
            );
        });
    }

    @Test
    public void testConvertMapWithUdtValueKeys() {
        UdtValue originUdtValue1 = createMockOriginUdtValue("text value 1", 42L, 3.1416);
        UdtValue originUdtValue2 = createMockOriginUdtValue("text value 2", 43L, 4.1416);
        Map<UdtValue, String> udtMap = new HashMap<>();
        udtMap.put(originUdtValue1, "value1");
        udtMap.put(originUdtValue2, "value2");

        Map<UdtValue, String> convertedMap = (Map<UdtValue, String>) UDTMapper.convert(udtMap, new Tuple2<>(originUDT, targetUDT));
        assertEquals(2, convertedMap.size());
        convertedMap.forEach((key, value) -> {
            assertTrue(value.equals("value1") || value.equals("value2"));
            assertAll(
                    () -> assertEquals(value.equals("value1") ? "text value 1" : "text value 2", key.get(CqlIdentifier.fromCql("text_value"), codecRegistry.codecFor(DataTypes.TEXT)), "text value"),
                    () -> assertEquals(value.equals("value1") ? 42L : 43L, (Long) key.get(CqlIdentifier.fromCql("long_value"), codecRegistry.codecFor(DataTypes.BIGINT)), "long value"),
                    () -> assertEquals(value.equals("value1") ? 3.1416 : 4.1416, key.get(CqlIdentifier.fromCql("double_value"), codecRegistry.codecFor(DataTypes.DOUBLE)), "double value")
            );
        });
    }

    private void assertUdtValueEquals(UdtValue udtValue) {
        assertAll(
                () -> assertEquals("text value", udtValue.get(CqlIdentifier.fromCql("text_value"), codecRegistry.codecFor(DataTypes.TEXT)), "text value"),
                () -> assertEquals(42L, (Long) udtValue.get(CqlIdentifier.fromCql("long_value"), codecRegistry.codecFor(DataTypes.BIGINT)), "long value"),
                () -> assertEquals(3.1416, udtValue.get(CqlIdentifier.fromCql("double_value"), codecRegistry.codecFor(DataTypes.DOUBLE)), "double value")
        );
    }

}
