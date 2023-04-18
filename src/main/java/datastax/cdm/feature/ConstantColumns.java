package datastax.cdm.feature;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.data.PKFactory;
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
    public boolean initialize(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        List<String> columnNames = getConstantColumnNames(propertyHelper);

        putStringList(Property.COLUMN_NAMES, columnNames);
        putMigrateDataTypeList(Property.COLUMN_TYPES, getConstantColumnTypes(propertyHelper));
        putString(Property.SPLIT_REGEX, getSplitRegex(propertyHelper));
        putStringList(Property.COLUMN_VALUES, getConstantColumnValues(propertyHelper));

        valid = isValid();
        isInitialized = true;
        isEnabled = valid && null!=columnNames && !columnNames.isEmpty();
        return valid;
    }

    @Override
    public PropertyHelper alterProperties(PropertyHelper helper, PKFactory pkFactory) {
        if (!valid) return null;
        pkFactory.registerTypes(getRawStringList(Property.COLUMN_NAMES), getRawMigrateDataTypeList(Property.COLUMN_TYPES));
        return helper;
    }

    public static List<String> getConstantColumnNames(PropertyHelper propertyHelper) {
        return propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES);
    }

    public static List<MigrateDataType> getConstantColumnTypes(PropertyHelper propertyHelper) {
        return propertyHelper.getMigrationTypeList(KnownProperties.CONSTANT_COLUMN_TYPES);
    }

    public static String getSplitRegex(PropertyHelper propertyHelper) {
        return propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX);
    }

    public static List<String> getConstantColumnValues(PropertyHelper propertyHelper) {
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

    public static MigrateDataType getConstantColumnType(PropertyHelper helper, String columnName) {
        List<String> constantColumnNames = getConstantColumnNames(helper);
        List<MigrateDataType> constantColumnTypes = getConstantColumnTypes(helper);
        if (null!=constantColumnNames && null!=constantColumnTypes && constantColumnNames.size() == constantColumnTypes.size()) {
            int index = constantColumnNames.indexOf(columnName);
            if (index >= 0)
                return constantColumnTypes.get(index);
        }
        return null;
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
