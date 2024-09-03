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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ExtractJson extends AbstractFeature {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private ObjectMapper mapper = new ObjectMapper();

    private String originColumnName = "";
    private String originJsonFieldName = "";
    private Integer originColumnIndex = -1;

    private String targetColumnName = "";
    private Integer targetColumnIndex = -1;
    private boolean overwriteTarget = false;

    @Override
    public boolean loadProperties(IPropertyHelper helper) {
        if (null == helper) {
            throw new IllegalArgumentException("helper is null");
        }

        originColumnName = getColumnName(helper, KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME);
        targetColumnName = getColumnName(helper, KnownProperties.EXTRACT_JSON_TARGET_COLUMN_MAPPING);
        overwriteTarget = helper.getBoolean(KnownProperties.EXTRACT_JSON_TARGET_OVERWRITE);

        // Convert columnToFieldMapping to targetColumnName and originJsonFieldName
        if (!targetColumnName.isBlank()) {
            String[] parts = targetColumnName.split("\\:");
            if (parts.length == 2) {
                originJsonFieldName = parts[0];
                targetColumnName = parts[1];
            } else {
                originJsonFieldName = targetColumnName;
            }
        }

        isValid = validateProperties();
        isEnabled = isValid && !originColumnName.isEmpty() && !targetColumnName.isEmpty();
        isLoaded = true;

        return isLoaded && isValid;
    }

    @Override
    protected boolean validateProperties() {
        if (StringUtils.isBlank(originColumnName) && StringUtils.isBlank(targetColumnName))
            return true;

        if (StringUtils.isBlank(originColumnName)) {
            logger.error("Origin column name is not set when Target ({}) is set", targetColumnName);
            return false;
        }

        if (StringUtils.isBlank(targetColumnName)) {
            logger.error("Target column name is not set when Origin ({}) is set", originColumnName);
            return false;
        }

        return true;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null == originTable || null == targetTable) {
            throw new IllegalArgumentException("Origin table and/or Target table is null");
        }
        if (!originTable.isOrigin()) {
            throw new IllegalArgumentException(originTable.getKeyspaceTable() + " is not an origin table");
        }
        if (targetTable.isOrigin()) {
            throw new IllegalArgumentException(targetTable.getKeyspaceTable() + " is not a target table");
        }

        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }
        if (!isEnabled)
            return true;

        // Initialize Origin variables
        List<Class> originBindClasses = originTable.extendColumns(Collections.singletonList(originColumnName));
        if (null == originBindClasses || originBindClasses.size() != 1 || null == originBindClasses.get(0)) {
            throw new IllegalArgumentException("Origin column " + originColumnName
                    + " is not found on the origin table " + originTable.getKeyspaceTable());
        } else {
            this.originColumnIndex = originTable.indexOf(originColumnName);
        }

        // Initialize Target variables
        List<Class> targetBindClasses = targetTable.extendColumns(Collections.singletonList(targetColumnName));
        if (null == targetBindClasses || targetBindClasses.size() != 1 || null == targetBindClasses.get(0)) {
            throw new IllegalArgumentException("Target column " + targetColumnName
                    + " is not found on the target table " + targetTable.getKeyspaceTable());
        } else {
            this.targetColumnIndex = targetTable.indexOf(targetColumnName);
        }

        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled ? "enabled" : "disabled");
        return true;
    }

    public Object extract(String jsonString) throws JsonMappingException, JsonProcessingException {
        if (StringUtils.isNotBlank(jsonString)) {
            return mapper.readValue(jsonString, Map.class).get(originJsonFieldName);
        }

        return null;
    }

    public Integer getOriginColumnIndex() {
        return isEnabled ? originColumnIndex : -1;
    }

    public Integer getTargetColumnIndex() {
        return isEnabled ? targetColumnIndex : -1;
    }

    public String getTargetColumnName() {
        return isEnabled ? targetColumnName : "";
    }

    public boolean overwriteTarget() {
        return overwriteTarget;
    }

    private String getColumnName(IPropertyHelper helper, String colName) {
        String columnName = CqlTable.unFormatName(helper.getString(colName));
        return (null == columnName) ? "" : columnName;
    }
}
