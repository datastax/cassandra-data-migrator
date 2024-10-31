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

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.Row;

public class Guardrail extends AbstractFeature {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final boolean logDebug = logger.isDebugEnabled();
    private final boolean logTrace = logger.isTraceEnabled();

    public static final String CLEAN_CHECK = "";
    public static final int BASE_FACTOR = 1000;
    private DecimalFormat decimalFormat = new DecimalFormat("0.###");

    private Double colSizeInKB;
    private CqlTable originTable;

    @Override
    public boolean loadProperties(IPropertyHelper propertyHelper) {
        Number property = propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB);
        if (null == property)
            this.colSizeInKB = 0.0;
        else
            this.colSizeInKB = property.doubleValue();

        isValid = validateProperties();
        isLoaded = true;
        isEnabled = (isValid && colSizeInKB > 0);
        return isValid;
    }

    @Override
    protected boolean validateProperties() {
        isValid = true;
        if (this.colSizeInKB < 0) {
            logger.error("{} must be greater than equal to zero, but is {}", KnownProperties.GUARDRAIL_COLSIZE_KB,
                    this.colSizeInKB);
            isValid = false;
        }

        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null == originTable || !originTable.isOrigin()) {
            logger.error("originTable is null, or is not an origin table");
            return false;
        }
        this.originTable = originTable;

        isValid = true;
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }

        if (logDebug)
            logger.debug("Guardrail is {}. colSizeInKB={}", isEnabled ? "enabled" : "disabled", colSizeInKB);

        return isValid;
    }

    private Map<String, Integer> check(Map<String, Integer> currentChecks, int targetIndex, Object targetValue) {
        int colSize = originTable.byteCount(targetIndex, targetValue);
        if (logTrace)
            logger.trace("Column {} at targetIndex {} has size {} bytes",
                    originTable.getColumnNames(false).get(targetIndex), targetIndex, colSize);
        if (colSize > colSizeInKB * BASE_FACTOR) {
            if (null == currentChecks)
                currentChecks = new HashMap<String, Integer>();
            currentChecks.put(originTable.getColumnNames(false).get(targetIndex), colSize);
        }
        return currentChecks;
    }

    public String guardrailChecks(Row row) {
        if (!isEnabled)
            return null;

        Map<String, Integer> largeColumns = null;
        for (int i = 0; i < originTable.getColumnNames(false).size(); i++) {
            Object targetObject = originTable.getAndConvertData(i, row);
            largeColumns = check(largeColumns, i, targetObject);
        }

        if (null == largeColumns || largeColumns.isEmpty())
            return CLEAN_CHECK;

        StringBuilder sb = new StringBuilder();
        sb.append("Large columns (KB): ");
        int colCount = 0;
        for (Map.Entry<String, Integer> entry : largeColumns.entrySet()) {
            if (colCount++ > 0)
                sb.append(",");
            sb.append(entry.getKey()).append("(").append(decimalFormat.format(entry.getValue() / BASE_FACTOR))
                    .append(")");
        }

        return sb.toString();
    }

}
