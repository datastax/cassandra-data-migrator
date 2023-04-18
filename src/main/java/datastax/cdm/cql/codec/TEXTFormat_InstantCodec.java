package datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRulesProvider;

public class TEXTFormat_InstantCodec extends AbstractBaseCodec<Instant> {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private final DateTimeFormatter formatter;
    private final ZoneOffset zoneOffset;

    public TEXTFormat_InstantCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        String formatString = propertyHelper.getString(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT);
        if (formatString == null || formatString.isEmpty()) {
            throw new IllegalArgumentException("Property " + KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT + " is required and cannot be empty.");
        }
        this.formatter = DateTimeFormatter.ofPattern(formatString);

        String zone = propertyHelper.getString(KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE);
        if (zone == null || !ZoneRulesProvider.getAvailableZoneIds().contains(zone)) {
            throw new IllegalArgumentException("Property " + KnownProperties.TRANSFORM_CODECS_TIMESTAMP_STRING_FORMAT_ZONE + " is required and must be a valid ZoneOffset.");
        }
        this.zoneOffset = ZoneId.of(zone).getRules().getOffset(Instant.now());
    }

    @Override
    public @NotNull GenericType<Instant> getJavaType() {
        return GenericType.INSTANT;
    }

    @Override
    public @NotNull DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(Instant value, @NotNull ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            String stringValue = formatter.format(LocalDateTime.ofInstant(value, zoneOffset));
            return TypeCodecs.TEXT.encode(stringValue, protocolVersion);
        }
    }

    @Override
    public Instant decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
        String stringValue = TypeCodecs.TEXT.decode(bytes, protocolVersion);
        return LocalDateTime.parse(stringValue, formatter).toInstant(zoneOffset);
    }

    @Override
    public @NotNull String format(Instant value) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(value, zoneOffset);
        return formatter.format(localDateTime);
    }

    @Override
    public Instant parse(String value) {
        LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
        return localDateTime.toInstant(zoneOffset);
    }

}
