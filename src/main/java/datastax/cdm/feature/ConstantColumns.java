package datastax.cdm.feature;

import datastax.cdm.job.MigrateDataType;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConstantColumns extends AbstractFeature {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public enum Property {
        COLUMN_NAMES,
        COLUMN_TYPES,
        COLUMN_VALUES,
        SPLIT_REGEX
    }

    private boolean valid = true;

    @Override
    public boolean initialize(PropertyHelper propertyHelper) {
        List<String> columnNames = propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES);
        putStringList(Property.COLUMN_NAMES, columnNames);

        List<MigrateDataType> columnTypes = propertyHelper.getMigrationTypeList(KnownProperties.CONSTANT_COLUMN_TYPES);
        putMigrateDataTypeList(Property.COLUMN_TYPES, columnTypes);

        String splitRegex = propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX);
        putString(Property.SPLIT_REGEX, splitRegex);

        List<String> columnValues = columnValues(
                propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_VALUES),
                splitRegex);
        putStringList(Property.COLUMN_VALUES, columnValues);

        valid = isValid();
        isInitialized = true;
        isEnabled = valid && null!=columnNames && !columnNames.isEmpty();
        return valid;
    }

    @Override
    public PropertyHelper alterProperties(PropertyHelper helper) {
        if (!valid) return null;
        return helper;
    }

    private List<String> columnValues(String columnValueString, String regexString) {
        List<String> columnValues = new ArrayList<>();
        if (null!=columnValueString && !columnValueString.isEmpty()) {
            if (null==regexString || regexString.isEmpty()) {
                logger.error("Constant column values are specified [{}], but no split regex is provided in property {}"
                        , columnValueString
                        , KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX);
            } else {
                columnValues = Arrays.asList(columnValueString.split(regexString));
            }
        }
        return columnValues;
    }

    private boolean isValid() {
        List<String> columnNames = getRawStringList(Property.COLUMN_NAMES);
        List<MigrateDataType> columnTypes = getRawMigrateDataTypeList(Property.COLUMN_TYPES);
        List<String> columnValues = getRawStringList(Property.COLUMN_VALUES);
        String regexString = getRawString(Property.SPLIT_REGEX);

        boolean haveColumnNames = null!=columnNames && !columnNames.isEmpty();
        boolean haveColumnTypes = null!=columnTypes && !columnTypes.isEmpty();
        boolean haveColumnValues = null!=columnValues && !columnValues.isEmpty();

        boolean valid = true;
        if ((!haveColumnNames && !haveColumnTypes && !haveColumnValues) ||
            ( haveColumnNames &&  haveColumnTypes &&  haveColumnValues)) {
            // These are the valid conditions...anything else is not valid
        }
        else {
            logger.error("Properties must all be empty, or all not empty: {}={}, {}={}, {}={} (split by {}={})",
                KnownProperties.CONSTANT_COLUMN_NAMES, columnNames,
                KnownProperties.CONSTANT_COLUMN_TYPES, columnTypes,
                KnownProperties.CONSTANT_COLUMN_VALUES, columnValues,
                KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, regexString);
            valid =  false;
        }

        if (haveColumnNames && haveColumnTypes && haveColumnValues &&
               (columnNames.size() != columnTypes.size() ||
                columnNames.size() != columnValues.size())) {
            logger.error("Values must have the same number of elements: {}={}, {}={}, {}={} (split by {}={})",
                    KnownProperties.CONSTANT_COLUMN_NAMES, columnNames,
                    KnownProperties.CONSTANT_COLUMN_TYPES, columnTypes,
                    KnownProperties.CONSTANT_COLUMN_VALUES, columnValues,
                    KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, regexString);
            valid =  false;
        }

        return valid;
    }
}
