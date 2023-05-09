package com.datastax.cdm.feature;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class WritetimeTTL extends AbstractFeature  {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final boolean logDebug = logger.isDebugEnabled();;

    private List<String> ttlNames;
    private boolean autoTTLNames;
    private List<String> writetimeNames;
    private boolean autoWritetimeNames;
    private Long customWritetime = 0L;
    private List<Integer> ttlSelectColumnIndexes = null;
    private List<Integer> writetimeSelectColumnIndexes = null;
    private Long filterMin;
    private Long filterMax;
    private boolean hasWriteTimestampFilter;

    @Override
    public boolean loadProperties(IPropertyHelper propertyHelper) {
        this.ttlNames = getTTLNames(propertyHelper);
        if (null!=this.ttlNames && !this.ttlNames.isEmpty()) {
            logger.info("PARAM -- TTLCols: {}", ttlNames);
        }

        this.autoTTLNames = propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO);
        this.writetimeNames = getWritetimeNames(propertyHelper);
        if (null!=this.writetimeNames && !this.writetimeNames.isEmpty()) {
            logger.info("PARAM -- WriteTimestampCols: {}", writetimeNames);
            this.autoTTLNames = false;
        }

        this.autoWritetimeNames = propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO);
        this.customWritetime = getCustomWritetime(propertyHelper);
        if (this.customWritetime > 0) {
            logger.info("PARAM -- {}: {} datetime is {} ", KnownProperties.TRANSFORM_CUSTOM_WRITETIME, customWritetime, Instant.ofEpochMilli(customWritetime / 1000));
            this.autoWritetimeNames = false;
        }

        this.filterMin = getMinFilter(propertyHelper);
        this.filterMax = getMaxFilter(propertyHelper);
        this.hasWriteTimestampFilter = (null != filterMin && null != filterMax && filterMin > 0 && filterMax > 0 && filterMax > filterMin);
        if (this.hasWriteTimestampFilter) {
            logger.info("PARAM -- {}: {} datetime is {} ", KnownProperties.FILTER_WRITETS_MIN, filterMin, Instant.ofEpochMilli(filterMin / 1000));
            logger.info("PARAM -- {}: {} datetime is {} ", KnownProperties.FILTER_WRITETS_MAX, filterMax, Instant.ofEpochMilli(filterMax / 1000));
        }

        isValid = validateProperties();
        isEnabled = isValid &&
                ((null != ttlNames && !ttlNames.isEmpty())
                || (null != writetimeNames && !writetimeNames.isEmpty())
                || customWritetime > 0);

        isLoaded = true;
        return isValid;
    }

    @Override
    protected boolean validateProperties() {
        isValid = true;
        validateCustomWritetimeProperties();
        validateFilterRangeProperties();
        validateTTLNames();
        validateWritetimeNames();
        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null==originTable) {
            throw new IllegalArgumentException("originTable is null");
        }
        if (!originTable.isOrigin()) {
            throw new IllegalArgumentException("Origin table is not an origin table");
        }

        if (originTable.isCounterTable()) {
            if (isEnabled) {
                logger.error("Counter table cannot specify TTL or WriteTimestamp columns as they cannot set on write");
                isValid = false;
                isEnabled = false;
                return false;
            }

            logger.info("Counter table does not support TTL or WriteTimestamp columns as they cannot set on write, so feature is disabled");
            return true;
        }

        isValid = true;
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }

        if (autoTTLNames) {
            this.ttlNames = originTable.getWritetimeTTLColumns();
        }

        if (autoWritetimeNames) {
            this.writetimeNames = originTable.getWritetimeTTLColumns();
        }

        validateTTLColumns(originTable);
        validateWritetimeColumns(originTable);

        if (hasWriteTimestampFilter && (null==writetimeNames || writetimeNames.isEmpty())) {
            logger.error("WriteTimestamp filter is configured but no WriteTimestamp columns are defined");
            isValid = false;
        }

        if (!isValid) isEnabled = false;
        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled?"enabled":"disabled");
        return isValid;
    }

    public static List<String> getTTLNames(IPropertyHelper propertyHelper) {
        return CqlTable.unFormatNames(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES));
    }

    public static List<String> getWritetimeNames(IPropertyHelper propertyHelper) {
        return CqlTable.unFormatNames(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES));
    }

    public static Long getCustomWritetime(IPropertyHelper propertyHelper) {
        Long cwt = propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME);
        return null==cwt ? 0L : cwt;
    }

    public static Long getMinFilter(IPropertyHelper propertyHelper) {
        return propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN);
    }

    public static Long getMaxFilter(IPropertyHelper propertyHelper) {
        return propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX);
    }

    public Long getCustomWritetime() { return customWritetime; }
    public boolean hasWriteTimestampFilter() { return isEnabled && hasWriteTimestampFilter; }
    public Long getMinWriteTimeStampFilter() { return (this.hasWriteTimestampFilter && null!=this.filterMin) ? this.filterMin : Long.MIN_VALUE; }
    public Long getMaxWriteTimeStampFilter() { return (this.hasWriteTimestampFilter && null!=this.filterMax) ? this.filterMax : Long.MAX_VALUE; }

    public boolean hasTTLColumns() { return null!=this.ttlSelectColumnIndexes && !this.ttlSelectColumnIndexes.isEmpty(); }
    public boolean hasWritetimeColumns() { return customWritetime>0 || null!=this.writetimeSelectColumnIndexes && !this.writetimeSelectColumnIndexes.isEmpty(); }

    public Long getLargestWriteTimeStamp(Row row) {
        if (logDebug) logger.debug("getLargestWriteTimeStamp: customWritetime={}, writetimeSelectColumnIndexes={}", customWritetime,writetimeSelectColumnIndexes);
        if (this.customWritetime > 0) return this.customWritetime;
        if (null==this.writetimeSelectColumnIndexes || this.writetimeSelectColumnIndexes.isEmpty()) return null;
        OptionalLong max = this.writetimeSelectColumnIndexes.stream()
                .mapToLong(row::getLong)
                .filter(Objects::nonNull)
                .max();
        return max.isPresent() ? max.getAsLong() : null;
    }

    public Integer getLargestTTL(Row row) {
        if (logDebug) logger.debug("getLargestTTL: ttlSelectColumnIndexes={}", ttlSelectColumnIndexes);
        if (null==this.ttlSelectColumnIndexes || this.ttlSelectColumnIndexes.isEmpty()) return null;
        OptionalInt max = this.ttlSelectColumnIndexes.stream()
                .mapToInt(row::getInt)
                .filter(Objects::nonNull)
                .max();
        return max.isPresent() ? max.getAsInt() : null;
    }

    private void validateTTLColumns(CqlTable originTable) {
        if (ttlNames == null || ttlNames.isEmpty()) {
            return;
        }

        List<String> newColumnNames = new ArrayList<>();
        List<DataType> newColumnDataTypes = new ArrayList<>();
        for (String ttlName : ttlNames) {
            int index = originTable.indexOf(ttlName);
            if (index < 0) {
                logger.error("TTL column {} is not present on origin table {}", ttlName, originTable.getKeyspaceName());
                isValid = false;
                return;
            } else {
                if (!originTable.isWritetimeTTLColumn(ttlName)) {
                    logger.error("TTL column {} is not a column which can provide a TTL on origin table {}", ttlName, originTable.getKeyspaceName());
                    isValid = false;
                    return;
                }
            }

            newColumnNames.add("TTL(" + ttlName + ")");
            newColumnDataTypes.add(DataTypes.INT);
        }

        originTable.extendColumns(newColumnNames, newColumnDataTypes);
        ttlSelectColumnIndexes = newColumnNames.stream()
                .mapToInt(originTable::indexOf)
                .boxed()
                .collect(Collectors.toList());
    }

    private void validateWritetimeColumns(CqlTable originTable) {
        if (writetimeNames == null || writetimeNames.isEmpty()) {
            return;
        }

        List<String> newColumnNames = new ArrayList<>();
        List<DataType> newColumnDataTypes = new ArrayList<>();
        for (String writetimeName : writetimeNames) {
            int index = originTable.indexOf(writetimeName);
            if (index < 0) {
                logger.error("Writetime column {} is not configured for origin table {}", writetimeName, originTable.getKeyspaceName());
                isValid = false;
                return;
            } else {
                if (!originTable.isWritetimeTTLColumn(writetimeName)) {
                    logger.error("Writetime column {} is not a column which can provide a WRITETIME on origin table {}", writetimeName, originTable.getKeyspaceName());
                    isValid = false;
                    return;
                }
            }

            newColumnNames.add("WRITETIME(" + writetimeName + ")");
            newColumnDataTypes.add(DataTypes.BIGINT);
        }

        originTable.extendColumns(newColumnNames, newColumnDataTypes);
        writetimeSelectColumnIndexes = new ArrayList<>();
        writetimeSelectColumnIndexes = newColumnNames.stream()
                .mapToInt(originTable::indexOf)
                .boxed()
                .collect(Collectors.toList());
    }

    private void validateTTLNames() {
        if (null!=ttlNames && ttlNames.size()==0) {
            logger.error("must be null or not empty");
            isValid = false;
        }
    }

    private void validateWritetimeNames() {
        if (null!=writetimeNames && writetimeNames.size()==0) {
            logger.error("must be null or not empty");
            isValid = false;
        }
    }

    private void validateCustomWritetimeProperties() {
        if (customWritetime < 0) {
            logger.warn("Custom Writetime {} is out of range, disabling by setting to 0", customWritetime);
            customWritetime = 0L;
        }
    }

    private void validateFilterRangeProperties() {
        if (filterMin != null && filterMin < 0) {
            logger.error("Filter Min {} is out of range", filterMin);
            isValid = false;
        }

        if (filterMax != null && filterMax < 0) {
            logger.error("Filter Max {} is out of range", filterMax);
            isValid = false;
        }

        if (filterMin != null && filterMax != null && filterMin > filterMax) {
            logger.error("Filter Min {} is greater than Filter Max {}", filterMin, filterMax);
            isValid = false;
        }
    }
}
