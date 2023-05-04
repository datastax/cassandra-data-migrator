package com.datastax.cdm.feature;

import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.cdm.properties.KnownProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        isEnabled = (null!=names && names.size() > 0);
        return isLoaded && isValid;
    }

    @Override
    protected boolean validateProperties() {
        if ((null == names  || names.isEmpty()) &&
            (null == values || values.isEmpty())) {
            isValid = true; // feature is disabled, which is valid
        }
        else {
            // both names and values must be set, not empty, and of the same size
            isValid = (null!= names && null!= values &&
                       names.size() > 0 &&
                       names.size() == values.size());
            if (!isValid)
                logger.error("Constant column names ({}) and values ({}) are of different sizes", names, values);
        }
        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null==targetTable) {
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
        if (!isEnabled) return true;

        this.bindClasses = targetTable.extendColumns(this.names);
        for (int i=0; i<bindClasses.size(); i++) {
            if (null == bindClasses.get(i)) {
                logger.error("Constant column {} is not found on the target table {}", names.get(i), targetTable.getKeyspaceTable());
                isValid = false;
            }
        }

        // Now we know all columns are valid, we can verify that the configured value is not empty and can be parsed
        if (isValid) {
            CodecRegistry codecRegistry = targetTable.getCodecRegistry();
            for (int i=0; i<values.size(); i++) {
                String value = values.get(i);
                if (null==value || value.isEmpty()) {
                    logger.error("Constant column value {} is null or empty", value);
                    isValid = false;
                } else {
                    DataType dataType = targetTable.getDataType(names.get(i));
                    TypeCodec<Object> codec = codecRegistry.codecFor(dataType);
                    try {
                        codec.parse(value);
                    } catch (Exception e) {
                        logger.error("Constant column value {} cannot be parsed as type {}", value, dataType.asCql(true, true));
                        isValid = false;
                    }
                }
            }
        }

        if (!isValid) isEnabled = false;
        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled?"enabled":"disabled");
        return isValid;
    }

    public List<String> getNames() { return isEnabled ? names : Collections.emptyList(); }
    public List<Class> getBindClasses() { return isEnabled ? bindClasses : Collections.emptyList(); }
    public List<String> getValues() { return isEnabled ? values : Collections.emptyList(); }

    public static List<String> getConstantColumnNames(IPropertyHelper propertyHelper) {
        return CqlTable.unFormatNames(propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES));
    }

    public static String getSplitRegex(IPropertyHelper propertyHelper) {
        return propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX);
    }

    public static List<String> getConstantColumnValues(IPropertyHelper propertyHelper) {
        String columnValueString = propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_VALUES);
        String regexString = getSplitRegex(propertyHelper);

        List<String> columnValues = new ArrayList<>();
        if (null!=columnValueString && !columnValueString.isEmpty()) {
            if (null==regexString || regexString.isEmpty()) {
                throw new RuntimeException("Constant column values are specified [" + columnValueString + "], but no split regex is provided in property " + KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX);
            } else {
                columnValues = Arrays.asList(columnValueString.split(regexString));
            }
        }
        return columnValues;
    }

}
