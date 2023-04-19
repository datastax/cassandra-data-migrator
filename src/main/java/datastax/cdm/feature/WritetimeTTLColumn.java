package datastax.cdm.feature;

import com.datastax.oss.driver.api.core.cql.Row;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class WritetimeTTLColumn extends AbstractFeature  {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public enum Property {
        TTL_INDEXES,
        WRITETIME_INDEXES,
        CUSTOM_WRITETIME
    }

    private List<Integer> ttlIndexes;
    private List<Integer> writetimeIndexes;
    private Long customWritetime = 0L;
    private List<Integer> ttlSelectColumnIndexes;
    private List<Integer> writetimeSelectColumnIndexes;

    @Override
    public boolean initialize(PropertyHelper propertyHelper, CqlHelper cqlHelper) {

        ttlSelectColumnIndexes = new ArrayList<>();
        writetimeSelectColumnIndexes = new ArrayList<>();

        this.ttlIndexes = propertyHelper.getIntegerList(KnownProperties.ORIGIN_TTL_INDEXES);
        if (null!=this.ttlIndexes && !this.ttlIndexes.isEmpty()) {
            putNumberList(Property.TTL_INDEXES, propertyHelper.getNumberList(KnownProperties.ORIGIN_TTL_INDEXES));
            logger.info("PARAM -- TTLCols: {}", ttlIndexes);
        }

        this.writetimeIndexes = propertyHelper.getIntegerList(KnownProperties.ORIGIN_WRITETIME_INDEXES);
        if (null!=this.writetimeIndexes && !this.writetimeIndexes.isEmpty()) {
            putNumberList(Property.WRITETIME_INDEXES, propertyHelper.getNumberList(KnownProperties.ORIGIN_WRITETIME_INDEXES));
            logger.info("PARAM -- WriteTimestampCols: {}", writetimeIndexes);
        }

        Long cwt = propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME);
        this.customWritetime = (null==cwt || cwt < 0) ? 0L : cwt;
        putNumber(Property.CUSTOM_WRITETIME,this.customWritetime);
        if (this.customWritetime > 0) {
            logger.info("PARAM -- {}: {} datetime is {} ", KnownProperties.TRANSFORM_CUSTOM_WRITETIME, customWritetime, Instant.ofEpochMilli(customWritetime / 1000));
        }

        isInitialized=true;
        isEnabled=((null != ttlIndexes && !ttlIndexes.isEmpty())
                || (null != writetimeIndexes && !writetimeIndexes.isEmpty())
                || customWritetime > 0);
        return true;
    }

    public void addTTLSelectColumnIndex(int index) {
        ttlSelectColumnIndexes.add(index);
    }

    public void addWritetimeSelectColumnIndex(int index) {
        writetimeSelectColumnIndexes.add(index);
    }

    public Long getLargestWriteTimeStamp(Row row) {
        if (this.customWritetime > 0) return this.customWritetime;
        if (null==this.writetimeSelectColumnIndexes || this.writetimeSelectColumnIndexes.isEmpty()) return null;
        OptionalLong max = this.writetimeSelectColumnIndexes.stream()
                .mapToLong(row::getLong)
                .filter(Objects::nonNull)
                .max();
        return max.isPresent() ? max.getAsLong() : null;
    }

    public Integer getLargestTTL(Row row) {
        if (null==this.ttlSelectColumnIndexes || this.ttlSelectColumnIndexes.isEmpty()) return null;
        OptionalInt max = this.ttlSelectColumnIndexes.stream()
                .mapToInt(row::getInt)
                .filter(Objects::nonNull)
                .max();
        return max.isPresent() ? max.getAsInt() : null;
    }

    public Long getCustomWritetime() {
        return customWritetime;
    }

}
