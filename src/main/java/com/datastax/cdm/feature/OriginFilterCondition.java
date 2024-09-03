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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;

public class OriginFilterCondition extends AbstractFeature {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private String filterCondition;

    @Override
    public boolean loadProperties(IPropertyHelper propertyHelper) {
        this.filterCondition = propertyHelper.getString(KnownProperties.FILTER_CQL_WHERE_CONDITION);
        isValid = validateProperties();
        isLoaded = true;
        isEnabled = (isValid && null != filterCondition && !filterCondition.isEmpty());
        return isValid;
    }

    @Override
    protected boolean validateProperties() {
        isValid = true;
        if (null == filterCondition || filterCondition.isEmpty())
            return isValid;

        String trimmedFilter = filterCondition.trim();
        if (trimmedFilter.isEmpty()) {
            logger.error("Provided filter contains only whitespace characters");
            isValid = false;
        }
        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        isValid = true;
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }
        if (null == filterCondition || filterCondition.isEmpty()) {
            isEnabled = false;
            return isValid;
        }

        if (!filterCondition.toUpperCase().startsWith("AND")) {
            filterCondition = " AND " + filterCondition;
        }

        // TODO: in future, we may want to validate the condition against the origin table via initializeAndValidate
        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled ? "enabled" : "disabled");
        return isValid;
    }

    public String getFilterCondition() {
        return null == filterCondition ? "" : filterCondition;
    }
}
