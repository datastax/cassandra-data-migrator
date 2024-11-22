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
package com.datastax.cdm.feature;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class WritetimeTTL extends AbstractFeature {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final boolean logDebug = logger.isDebugEnabled();;

    private List<String> ttlNames;
    private boolean autoTTLNames;
    private List<String> writetimeNames;
    private boolean autoWritetimeNames;
    private Long customWritetime = 0L;
    private Long customTTL = 0L;
    private List<Integer> ttlSelectColumnIndexes = null;
    private List<Integer> writetimeSelectColumnIndexes = null;
    private Long filterMin;
    private Long filterMax;
    private boolean hasWriteTimestampFilter;
    private Long writetimeIncrement;
    private boolean allowCollectionsForWritetimeTTL;

    @Override
    public boolean loadProperties(IPropertyHelper propertyHelper) {
        this.autoTTLNames = propertyHelper.getBoolean(KnownProperties.ORIGIN_TTL_AUTO);
        this.ttlNames = getTTLNames(propertyHelper);
        if (null != this.ttlNames && !this.ttlNames.isEmpty()) {
            logger.info("PARAM -- TTLCols: {}", ttlNames);
            this.autoTTLNames = false;
        }

        this.autoWritetimeNames = propertyHelper.getBoolean(KnownProperties.ORIGIN_WRITETIME_AUTO);
        this.writetimeNames = getWritetimeNames(propertyHelper);
        if (null != this.writetimeNames && !this.writetimeNames.isEmpty()) {
            logger.info("PARAM -- WriteTimestampCols: {}", writetimeNames);
            this.autoWritetimeNames = false;
        }
        allowCollectionsForWritetimeTTL = propertyHelper.getBoolean(KnownProperties.ALLOW_COLL_FOR_WRITETIME_TTL_CALC);
        this.customWritetime = getCustomWritetime(propertyHelper);
        if (this.customWritetime > 0) {
            logger.info("PARAM -- {}: {} datetime is {} ", KnownProperties.TRANSFORM_CUSTOM_WRITETIME, customWritetime,
                    Instant.ofEpochMilli(customWritetime / 1000));
        }

        this.writetimeIncrement = propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME_INCREMENT);

        this.customTTL = getCustomTTL(propertyHelper);
        if (this.customTTL > 0) {
            logger.info("PARAM -- {} is set to TTL {} ", KnownProperties.TRANSFORM_CUSTOM_TTL, customTTL);
        }

        this.filterMin = getMinFilter(propertyHelper);
        this.filterMax = getMaxFilter(propertyHelper);
        this.hasWriteTimestampFilter = (null != filterMin && null != filterMax && filterMin > 0 && filterMax > 0
                && filterMax > filterMin);
        if (this.hasWriteTimestampFilter) {
            logger.info("PARAM -- {}: {} datetime is {} ", KnownProperties.FILTER_WRITETS_MIN, filterMin,
                    Instant.ofEpochMilli(filterMin / 1000));
            logger.info("PARAM -- {}: {} datetime is {} ", KnownProperties.FILTER_WRITETS_MAX, filterMax,
                    Instant.ofEpochMilli(filterMax / 1000));
        }

        isValid = validateProperties();
        isEnabled = isValid
                && ((null != ttlNames && !ttlNames.isEmpty()) || (null != writetimeNames && !writetimeNames.isEmpty())
                        || autoTTLNames || autoWritetimeNames || customWritetime > 0 || customTTL > 0);

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

        if (null == this.writetimeIncrement || this.writetimeIncrement < 0L) {
            logger.error(KnownProperties.TRANSFORM_CUSTOM_WRITETIME_INCREMENT
                    + " must be set to a value greater than or equal to zero");
            isValid = false;
        }

        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null == originTable) {
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
            }

            logger.info(
                    "Counter table does not support TTL or WriteTimestamp columns as they cannot set on write, so feature is disabled");
            return true;
        }

        isValid = true;
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }

        if (autoTTLNames) {
            this.ttlNames = originTable.getWritetimeTTLColumns();
            logger.info("PARAM -- Automatic TTLCols: {}", ttlNames);
        }

        if (autoWritetimeNames) {
            this.writetimeNames = originTable.getWritetimeTTLColumns();
            logger.info("PARAM -- Automatic WriteTimestampCols: {}", writetimeNames);
        }

        validateTTLColumns(originTable);
        validateWritetimeColumns(originTable);

        if (hasWriteTimestampFilter && (null == writetimeNames || writetimeNames.isEmpty())) {
            logger.error("WriteTimestamp filter is configured but no WriteTimestamp columns are defined");
            isValid = false;
        }

        if (this.writetimeIncrement == 0L && (null != writetimeNames && !writetimeNames.isEmpty())
                && originTable.hasUnfrozenList()) {
            logger.warn("Origin table has at least one unfrozen List, and "
                    + KnownProperties.TRANSFORM_CUSTOM_WRITETIME_INCREMENT
                    + " is set to zero; this may result in duplicate list entries on reruns or validation with autocorrect.");
        }

        if (!isValid)
            isEnabled = false;
        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled ? "enabled" : "disabled");
        return isValid;
    }

    public static List<String> getTTLNames(IPropertyHelper propertyHelper) {
        return CqlTable.unFormatNames(propertyHelper.getStringList(KnownProperties.ORIGIN_TTL_NAMES));
    }

    public static List<String> getWritetimeNames(IPropertyHelper propertyHelper) {
        return CqlTable.unFormatNames(propertyHelper.getStringList(KnownProperties.ORIGIN_WRITETIME_NAMES));
    }

    protected static Long getCustomWritetime(IPropertyHelper propertyHelper) {
        Long cwt = propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_WRITETIME);
        return null == cwt ? 0L : cwt;
    }

    protected static Long getCustomTTL(IPropertyHelper propertyHelper) {
        Long cttl = propertyHelper.getLong(KnownProperties.TRANSFORM_CUSTOM_TTL);
        return null == cttl ? 0L : cttl;
    }

    public static Long getMinFilter(IPropertyHelper propertyHelper) {
        return propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MIN);
    }

    public static Long getMaxFilter(IPropertyHelper propertyHelper) {
        return propertyHelper.getLong(KnownProperties.FILTER_WRITETS_MAX);
    }

    public Long getCustomWritetime() {
        return customWritetime;
    }

    public Long getCustomTTL() {
        return customTTL;
    }

    public boolean hasWriteTimestampFilter() {
        return isEnabled && hasWriteTimestampFilter;
    }

    public Long getMinWriteTimeStampFilter() {
        return (this.hasWriteTimestampFilter && null != this.filterMin) ? this.filterMin : Long.MIN_VALUE;
    }

    public Long getMaxWriteTimeStampFilter() {
        return (this.hasWriteTimestampFilter && null != this.filterMax) ? this.filterMax : Long.MAX_VALUE;
    }

    public boolean hasTTLColumns() {
        return customTTL > 0 || null != this.ttlSelectColumnIndexes && !this.ttlSelectColumnIndexes.isEmpty();
    }

    public boolean hasWritetimeColumns() {
        return customWritetime > 0
                || null != this.writetimeSelectColumnIndexes && !this.writetimeSelectColumnIndexes.isEmpty();
    }

    public Long getLargestWriteTimeStamp(Row row) {
        if (logDebug)
            logger.debug("getLargestWriteTimeStamp: writetimeSelectColumnIndexes={}", writetimeSelectColumnIndexes);
        if (null == this.writetimeSelectColumnIndexes || this.writetimeSelectColumnIndexes.isEmpty())
            return null;

        OptionalLong max = (allowCollectionsForWritetimeTTL) ? getMaxWriteTimeStampForCollections(row)
                : getMaxWriteTimeStamp(row);

        return max.isPresent() ? max.getAsLong() + this.writetimeIncrement : null;
    }

    private OptionalLong getMaxWriteTimeStampForCollections(Row row) {
        return this.writetimeSelectColumnIndexes.stream().map(col -> {
            if (row.getType(col).equals(DataTypes.BIGINT))
                return Arrays.asList(row.getLong(col));
            return row.getList(col, BigInteger.class).stream().filter(Objects::nonNull).map(BigInteger::longValue)
                    .collect(Collectors.toList());
        }).flatMap(List::stream).filter(Objects::nonNull).mapToLong(Long::longValue).max();
    }

    private OptionalLong getMaxWriteTimeStamp(Row row) {
        return this.writetimeSelectColumnIndexes.stream().filter(Objects::nonNull).mapToLong(row::getLong).max();
    }

    public Integer getLargestTTL(Row row) {
        if (logDebug)
            logger.debug("getLargestTTL: ttlSelectColumnIndexes={}", ttlSelectColumnIndexes);
        if (null == this.ttlSelectColumnIndexes || this.ttlSelectColumnIndexes.isEmpty())
            return null;

        OptionalInt max = (allowCollectionsForWritetimeTTL) ? getMaxTTLForCollections(row) : getMaxTTL(row);

        return max.isPresent() ? max.getAsInt() : 0;
    }

    private OptionalInt getMaxTTLForCollections(Row row) {
        return this.ttlSelectColumnIndexes.stream().map(col -> {
            if (row.getType(col).equals(DataTypes.INT))
                return Arrays.asList(row.getInt(col));
            return row.getList(col, Integer.class).stream().filter(Objects::nonNull).collect(Collectors.toList());
        }).flatMap(List::stream).filter(Objects::nonNull).mapToInt(Integer::intValue).max();
    }

    private OptionalInt getMaxTTL(Row row) {
        return this.ttlSelectColumnIndexes.stream().filter(Objects::nonNull).mapToInt(row::getInt).max();
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
                    logger.error("TTL column {} is not a column which can provide a TTL on origin table {}", ttlName,
                            originTable.getKeyspaceName());
                    isValid = false;
                    return;
                }
            }

            newColumnNames.add("TTL(" + CqlTable.formatName(ttlName) + ")");
            newColumnDataTypes.add(DataTypes.INT);
        }

        originTable.extendColumns(newColumnNames, newColumnDataTypes);
        ttlSelectColumnIndexes = newColumnNames.stream().mapToInt(originTable::indexOf).boxed()
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
                logger.error("Writetime column {} is not configured for origin table {}", writetimeName,
                        originTable.getKeyspaceName());
                isValid = false;
                return;
            } else {
                if (!originTable.isWritetimeTTLColumn(writetimeName)) {
                    logger.error("Writetime column {} is not a column which can provide a WRITETIME on origin table {}",
                            writetimeName, originTable.getKeyspaceName());
                    isValid = false;
                    return;
                }
            }

            newColumnNames.add("WRITETIME(" + CqlTable.formatName(writetimeName) + ")");
            newColumnDataTypes.add(DataTypes.BIGINT);
        }

        originTable.extendColumns(newColumnNames, newColumnDataTypes);
        writetimeSelectColumnIndexes = new ArrayList<>();
        writetimeSelectColumnIndexes = newColumnNames.stream().mapToInt(originTable::indexOf).boxed()
                .collect(Collectors.toList());
    }

    private void validateTTLNames() {
        if (null != ttlNames && ttlNames.size() == 0) {
            logger.error("must be null or not empty");
            isValid = false;
        }
    }

    private void validateWritetimeNames() {
        if (null != writetimeNames && writetimeNames.size() == 0) {
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

    public List<String> getTtlNames() {
        return ttlNames;
    }

    public List<String> getWritetimeNames() {
        return writetimeNames;
    }
}
