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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.type.DataType;

public class ExplodeMap extends AbstractFeature {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private String originColumnName = "";
    private Integer originColumnIndex = -1;

    private String keyColumnName = "";
    private Integer keyColumnIndex = -1;

    private String valueColumnName = "";
    private Integer valueColumnIndex = -1;

    protected CqlConversion keyConversion = null;
    protected CqlConversion valueConversion = null;

    @Override
    public boolean loadProperties(IPropertyHelper helper) {
        if (null == helper) {
            throw new IllegalArgumentException("helper is null");
        }

        this.originColumnName = getOriginColumnName(helper);
        this.keyColumnName = getKeyColumnName(helper);
        this.valueColumnName = getValueColumnName(helper);

        isValid = validateProperties();

        isEnabled = isValid && !originColumnName.isEmpty() && !keyColumnName.isEmpty() && !valueColumnName.isEmpty();

        isLoaded = true;
        return isLoaded && isValid;
    }

    @Override
    protected boolean validateProperties() {
        isValid = true;
        if ((null == originColumnName || originColumnName.isEmpty())
                && (null == keyColumnName || keyColumnName.isEmpty())
                && (null == valueColumnName || valueColumnName.isEmpty()))
            return true;

        if (null == originColumnName || originColumnName.isEmpty()) {
            logger.error("Origin column name is not set when Key ({}) and/or Value ({}) are set", keyColumnName,
                    valueColumnName);
            isValid = false;
        }

        if (null == keyColumnName || keyColumnName.isEmpty()) {
            logger.error("Key column name is not set when Origin ({}) and/or Value ({}) are set", originColumnName,
                    valueColumnName);
            isValid = false;
        }

        if (null == valueColumnName || valueColumnName.isEmpty()) {
            logger.error("Value column name is not set when Origin ({}) and/or Key ({}) are set", originColumnName,
                    keyColumnName);
            isValid = false;
        }

        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null == originTable || null == targetTable) {
            throw new IllegalArgumentException("originTable and/or targetTable is null");
        }
        if (!originTable.isOrigin()) {
            throw new IllegalArgumentException("Origin table is not an origin table");
        }
        if (targetTable.isOrigin()) {
            throw new IllegalArgumentException("Target table is not a target table");
        }

        isValid = true;
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }
        if (!isEnabled)
            return true;

        // Initialize Origin variables
        List<Class> originBindClasses = originTable.extendColumns(Collections.singletonList(originColumnName));
        if (null == originBindClasses || originBindClasses.size() != 1 || null == originBindClasses.get(0)) {
            logger.error("Origin column {} is not found on the origin table {}", originColumnName,
                    originTable.getKeyspaceTable());
            isValid = false;
        } else {
            if (!CqlData.Type.MAP.equals(CqlData.toType(originTable.getDataType(originColumnName)))) {
                logger.error("Origin column {} is not a map, it is {}", originColumnName,
                        originBindClasses.get(0).getName());
                isValid = false;
            } else {
                this.originColumnIndex = originTable.indexOf(originColumnName);
            }
        }

        // Initialize Target variables
        List<Class> targetBindClasses = targetTable.extendColumns(Arrays.asList(keyColumnName, valueColumnName));
        if (null == targetBindClasses || targetBindClasses.size() != 2 || null == targetBindClasses.get(0)
                || null == targetBindClasses.get(1)) {
            if (null == targetBindClasses.get(0))
                logger.error("Target key column {} is not found on the target table {}", keyColumnName,
                        targetTable.getKeyspaceTable());
            if (null == targetBindClasses.get(1))
                logger.error("Target value column {} is not found on the target table {}", valueColumnName,
                        targetTable.getKeyspaceTable());
            isValid = false;
        } else {
            this.keyColumnIndex = targetTable.indexOf(keyColumnName);
            this.valueColumnIndex = targetTable.indexOf(valueColumnName);
        }

        DataType originDataType = originTable.getDataType(originColumnName);
        if (CqlData.toType(originDataType) != CqlData.Type.MAP) {
            logger.error("Origin column {} is not a map, it is {}", originColumnName, originDataType);
            isValid = false;
        }

        if (isValid) {
            // Compute the target conversions, as these columns will not have a corresponding index
            List<DataType> originMapTypes = CqlData.extractDataTypesFromCollection(originDataType);

            DataType keyDataType = targetTable.getDataType(keyColumnName);
            keyConversion = new CqlConversion(originMapTypes.get(0), keyDataType, targetTable.getCodecRegistry());

            DataType valueDataType = targetTable.getDataType(valueColumnName);
            valueConversion = new CqlConversion(originMapTypes.get(1), valueDataType, targetTable.getCodecRegistry());
        }

        if (isEnabled && logger.isTraceEnabled()) {
            logger.trace("Origin column {} is at index {}", originColumnName, originColumnIndex);
            logger.trace("Target key column {} is at index {} with conversion {}", keyColumnName, keyColumnIndex,
                    keyConversion);
            logger.trace("Target value column {} is at index {} with conversion {}", valueColumnName, valueColumnIndex,
                    valueConversion);
        }

        if (!isValid)
            isEnabled = false;
        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled ? "enabled" : "disabled");
        return isValid;
    }

    public Set<Map.Entry<Object, Object>> explode(Map<Object, Object> map) {
        if (map == null) {
            return null;
        }
        return map.entrySet().stream().map(this::applyConversions).collect(Collectors.toSet());
    }

    private Map.Entry<Object, Object> applyConversions(Map.Entry<Object, Object> entry) {
        Object key = entry.getKey();
        Object value = entry.getValue();

        if (keyConversion != null) {
            key = keyConversion.convert(key);
        }

        if (valueConversion != null) {
            value = valueConversion.convert(value);
        }

        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public String getOriginColumnName() {
        return isEnabled ? originColumnName : "";
    }

    public Integer getOriginColumnIndex() {
        return isEnabled ? originColumnIndex : -1;
    }

    public String getKeyColumnName() {
        return isEnabled ? keyColumnName : "";
    }

    public Integer getKeyColumnIndex() {
        return isEnabled ? keyColumnIndex : -1;
    }

    public String getValueColumnName() {
        return isEnabled ? valueColumnName : "";
    }

    public Integer getValueColumnIndex() {
        return isEnabled ? valueColumnIndex : -1;
    }

    public static String getOriginColumnName(IPropertyHelper helper) {
        if (null == helper) {
            throw new IllegalArgumentException("helper is null");
        }
        String columnName = CqlTable.unFormatName(helper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME));
        return (null == columnName) ? "" : columnName;
    }

    public static String getKeyColumnName(IPropertyHelper helper) {
        if (null == helper) {
            throw new IllegalArgumentException("helper is null");
        }
        String columnName = CqlTable.unFormatName(helper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME));
        return (null == columnName) ? "" : columnName;
    }

    public static String getValueColumnName(IPropertyHelper helper) {
        if (null == helper) {
            throw new IllegalArgumentException("helper is null");
        }
        String columnName = CqlTable
                .unFormatName(helper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME));
        return (null == columnName) ? "" : columnName;
    }
}
