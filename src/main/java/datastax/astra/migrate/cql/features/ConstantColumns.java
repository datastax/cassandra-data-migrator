package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.apache.commons.lang.StringUtils;
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
        TARGET_PRIMARY_TYPES_WITHOUT_CONSTANT
    }

    @Override
    public boolean initialize(PropertyHelper propertyHelper) {
        List<String> columnNames = propertyHelper.getStringList(KnownProperties.CONSTANT_COLUMN_NAMES);
        putStringList(Property.COLUMN_NAMES, columnNames);

        List<MigrateDataType> columnTypes = propertyHelper.getMigrationTypeList(KnownProperties.CONSTANT_COLUMN_TYPES);
        putMigrateDataTypeList(Property.COLUMN_TYPES, columnTypes);

        List<String> columnValues = columnValues(
                propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_VALUES),
                propertyHelper.getString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX));
        putStringList(Property.COLUMN_VALUES, columnValues);

        List<MigrateDataType> targetPrimaryKeyTypesWithoutConstantColumns =
                targetPrimaryKeyTypesWithoutConstantColumns(
                        columnNames,
                        propertyHelper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES),
                        propertyHelper.getStringList(KnownProperties.TARGET_PRIMARY_KEY));
        putMigrateDataTypeList(Property.TARGET_PRIMARY_TYPES_WITHOUT_CONSTANT, targetPrimaryKeyTypesWithoutConstantColumns);

        isInitialized = true;
        if (!isValid(propertyHelper)) return false;
        isEnabled = null!=columnNames && !columnNames.isEmpty();
        return true;
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

    private List<MigrateDataType> targetPrimaryKeyTypesWithoutConstantColumns(List<String> columnNames, List<MigrateDataType> targetPrimaryKeyTypes, List<String> targetPrimaryKeyNames) {
        List<MigrateDataType> rtn = new ArrayList<>();
        if (null!=columnNames && !columnNames.isEmpty()) {
            if (null==targetPrimaryKeyTypes || null==targetPrimaryKeyNames) {
                if (null==targetPrimaryKeyTypes)
                    logger.error("Target primary key types are not specified in property {}", KnownProperties.TARGET_PRIMARY_KEY_TYPES);
                if (null==targetPrimaryKeyNames)
                    logger.error("Target primary key names are not specified in property {}", KnownProperties.TARGET_PRIMARY_KEY);
            }
            else {
                for (String keyName : targetPrimaryKeyNames) {
                    if (!columnNames.contains(keyName)) {
                        rtn.add(targetPrimaryKeyTypes.get(targetPrimaryKeyNames.indexOf(keyName)));
                    }
                }
            }
        }
        return rtn;
    }

    private boolean isValid(PropertyHelper propertyHelper) {
        List<String> columnNames = getRawStringList(Property.COLUMN_NAMES);
        List<MigrateDataType> columnTypes = getRawMigrateDataTypeList(Property.COLUMN_TYPES);
        List<String> columnValues = getRawStringList(Property.COLUMN_VALUES);
        List<MigrateDataType> targetPrimaryKeyTypesWithoutConstantColumns = getRawMigrateDataTypeList(Property.TARGET_PRIMARY_TYPES_WITHOUT_CONSTANT);

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
                KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, propertyHelper.getAsString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX));
            valid =  false;
        }

        if (haveColumnNames && haveColumnTypes && haveColumnValues &&
               (columnNames.size() != columnTypes.size() ||
                columnNames.size() != columnValues.size())) {
            logger.error("Values must have the same number of elements: {}={}, {}={}, {}={} (split by {}={})",
                    KnownProperties.CONSTANT_COLUMN_NAMES, columnNames,
                    KnownProperties.CONSTANT_COLUMN_TYPES, columnTypes,
                    KnownProperties.CONSTANT_COLUMN_VALUES, columnValues,
                    KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX, propertyHelper.getAsString(KnownProperties.CONSTANT_COLUMN_SPLIT_REGEX));
            valid =  false;
        }

        if (null==targetPrimaryKeyTypesWithoutConstantColumns || targetPrimaryKeyTypesWithoutConstantColumns.isEmpty()) {
            logger.warn("There are no primary key columns specified in property {} that are not constant columns.  This may be intentional, but it is unusual."
                    , KnownProperties.TARGET_PRIMARY_KEY);
        }

        return valid;
    }
}
