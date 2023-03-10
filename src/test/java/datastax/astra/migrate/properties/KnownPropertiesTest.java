package datastax.astra.migrate.properties;

import org.junit.Test;
import static org.junit.Assert.*;

public class KnownPropertiesTest {

    @Test
    public void getDefault_knownDefault() {
        assertEquals("text",KnownProperties.getDefault(KnownProperties.TEST_STRING));
    }

    @Test
    public void getDefault_knownNoDefault() {
        assertNull(KnownProperties.getDefault(KnownProperties.TEST_STRING_NO_DEFAULT));
    }

    @Test
    public void getDefault_unknownValue() {
        assertNull(KnownProperties.getDefault("unknown"));
    }

    @Test
    public void getDefault_nullKey() {
        assertNull(KnownProperties.getDefault(null));
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

}