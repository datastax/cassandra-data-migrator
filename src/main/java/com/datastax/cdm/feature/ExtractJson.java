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
	private Integer originColumnIndex = -1;

	private String targetColumnName = "";
	private Integer targetColumnIndex = -1;

	@Override
	public boolean loadProperties(IPropertyHelper helper) {
		if (null == helper) {
			throw new IllegalArgumentException("helper is null");
		}

		this.originColumnName = getColumnName(helper, KnownProperties.EXTRACT_JSON_ORIGIN_COLUMN_NAME);
		this.targetColumnName = getColumnName(helper, KnownProperties.EXTRACT_JSON_TARGET_COLUMN_NAME);
		logger.warn("Origin column name ({}) ", originColumnName);
		logger.warn("Target column name ({}) ", targetColumnName);

		isValid = validateProperties();
		logger.warn("ExtractJson valid ({}) ", isValid);

		isEnabled = isValid && !originColumnName.isEmpty() && !targetColumnName.isEmpty();
		logger.warn("ExtractJson isEnabled ({}) ", isEnabled);

		isLoaded = true;
		return isLoaded && isValid;
	}

	@Override
	protected boolean validateProperties() {
		isValid = true;
		if ((null == originColumnName || originColumnName.isEmpty())
				&& (null == targetColumnName || targetColumnName.isEmpty()))
			return true;

		if (null == originColumnName || originColumnName.isEmpty()) {
			logger.error("Origin column name is not set when Target ({}) are set", targetColumnName);
			isValid = false;
		}

		if (null == targetColumnName || targetColumnName.isEmpty()) {
			logger.error("Target column name is not set when Origin ({}) are set", originColumnName);
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
			this.originColumnIndex = originTable.indexOf(originColumnName);
		}

		// Initialize Target variables
		List<Class> targetBindClasses = targetTable.extendColumns(Arrays.asList(targetColumnName));
		if (null == targetBindClasses || targetBindClasses.size() != 1 || null == targetBindClasses.get(0)) {
			if (null == targetBindClasses.get(0))
				logger.error("Target column {} is not found on the target table {}", targetColumnName,
						targetTable.getKeyspaceTable());
			isValid = false;
		} else {
			this.targetColumnIndex = targetTable.indexOf(targetColumnName);
		}
		logger.warn("ExtractJson originColumnIndex ({}) ", originColumnIndex);
		logger.warn("ExtractJson targetColumnIndex ({}) ", targetColumnIndex);

		if (isEnabled && logger.isTraceEnabled()) {
			logger.trace("Origin column {} is at index {}", originColumnName, originColumnIndex);
			logger.trace("Target column {} is at index {}", targetColumnName, targetColumnIndex);
		}

		if (!isValid)
			isEnabled = false;
		logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled ? "enabled" : "disabled");
		return isValid;
	}

	public Object extract(String jsonString, String field) throws JsonMappingException, JsonProcessingException {
		if (StringUtils.isNotBlank(jsonString)) {
			return mapper.readValue(jsonString, Map.class).get(field);
		}

		return "";
	}
	
	public Integer getOriginColumnIndex() {
		return isEnabled ? originColumnIndex : -1;
	}

	public String getTargetColumnName() {
		return isEnabled ? targetColumnName : "";
	}

	public static String getColumnName(IPropertyHelper helper, String colName) {
		if (null == helper) {
			throw new IllegalArgumentException("helper is null");
		}
		String columnName = CqlTable.unFormatName(helper.getString(colName));
		return (null == columnName) ? "" : columnName;
	}
}
