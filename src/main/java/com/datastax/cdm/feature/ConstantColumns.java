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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

public class ConstantColumns extends AbstractFeature {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private List<String> names = null;
    private List<Class> bindClasses = null;
    private List<String> values = null;

    @Override
    public boolean loadProperties(IPropertyHelper propertyHelper) {
        this.names = getConstantColumnNames(propertyHelper);
        this.values = getConstantColumnValues(propertyHelper);

        isLoaded = true;
        isValid = validateProperties();
        isEnabled = (null != names && names.size() > 0);
        return isLoaded && isValid;
    }

    @Override
    protected boolean validateProperties() {
        if ((null == names || names.isEmpty()) && (null == values || values.isEmpty())) {
            return true; // feature is disabled, which is valid
        }
        // both names and values must be set, not empty, and of the same size
        if (null == names || null == values || names.size() == 0 || names.size() != values.size()) {
            logger.error("Constant column names ({}) and values ({}) are of different sizes", names, values);
            return false;
        }

        return true;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null == targetTable) {
            throw new IllegalArgumentException("targetTable is null");
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

        this.bindClasses = targetTable.extendColumns(this.names);
        for (int i = 0; i < bindClasses.size(); i++) {
            if (null == bindClasses.get(i)) {
                logger.error("Constant column {} is not found on the target table {}", names.get(i),
                        targetTable.getKeyspaceTable());
                isValid = false;
            }
        }

        // Now we know all columns are valid, we can verify that the configured value is not empty and can be parsed
        if (isValid) {
            CodecRegistry codecRegistry = targetTable.getCodecRegistry();
            for (int i = 0; i < values.size(); i++) {
                String value = values.get(i);
                if (null == value || value.isEmpty()) {
                    logger.error("Constant column value {} is null or empty", value);
                    isValid = false;
                } else {
                    DataType dataType = targetTable.getDataType(names.get(i));
                    TypeCodec<Object> codec = codecRegistry.codecFor(dataType);
                    try {
                        codec.parse(value);
                    } catch (Exception e) {
                        logger.error("Constant column value {} cannot be parsed as type {}", value,
                                dataType.asCql(true, true));
                        isValid = false;
                    }
                }
            }
        }

        if (!isValid)
            isEnabled = false;
        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled ? "enabled" : "disabled");
        return isValid;
    }

    public List<String> getNames() {
        return isEnabled ? names : Collections.emptyList();
    }

    public List<Class> getBindClasses() {
        return isEnabled ? bindClasses : Collections.emptyList();
    }

    public List<String> getValues() {
        return isEnabled ? values : Collections.emptyList();
    }

    private static List<String> getConstantColumnNames(IPropertyHelper propertyHelper) {
        return CqlTable.unFormatNames(propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES));
    }

    private static List<String> getConstantColumnValues(IPropertyHelper propertyHelper) {
        String columnValueString = propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_VALUES);
        String regexString = propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX);

        if (null != columnValueString && !columnValueString.isEmpty()) {
            if (null == regexString || regexString.isEmpty()) {
                throw new RuntimeException("Constant column values are specified [" + columnValueString
                        + "], but no split regex is provided in property "
                        + KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX);
            } else {
                return Arrays.asList(columnValueString.split(regexString));
            }
        }
        return Collections.emptyList();
    }

}
