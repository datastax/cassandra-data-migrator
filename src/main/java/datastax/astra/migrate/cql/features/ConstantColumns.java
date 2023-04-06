package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
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
        SPLIT_REGEX,
        WHERE_CLAUSE
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

        setWhereClause(propertyHelper);

        valid = isValid();
        isInitialized = true;
        isEnabled = valid && null!=columnNames && !columnNames.isEmpty();
        return valid;
    }

    @Override
    public PropertyHelper alterProperties(PropertyHelper helper) {
        if (!valid) return null;
        if (!isEnabled) return helper;

        clean_targetPK(helper);

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

    private void setWhereClause(PropertyHelper helper) {
        if (!isValid()) return;

        List<String> columnNames = getRawStringList(Property.COLUMN_NAMES);
        List<String> columnValues = getRawStringList(Property.COLUMN_VALUES);

        if (null != columnNames && !columnNames.isEmpty()
                && null != columnValues && !columnValues.isEmpty()) {
            List<String> targetPKNames = helper.getStringList(KnownProperties.TARGET_PRIMARY_KEY);

            String whereClause = "";
            for (String columnName : columnNames) {
                if (null!=targetPKNames && targetPKNames.contains(columnName)) {
                    if (!whereClause.isEmpty())
                        whereClause += " AND ";
                    whereClause += columnName + "=" + columnValues.get(columnNames.indexOf(columnName));
                }
            }
            putString(Property.WHERE_CLAUSE, " AND " + whereClause);
        }
    }

    // Constant columns do not belong on the PK, as they are hard-coded to WHERE_CLAUSE
    private void clean_targetPK(PropertyHelper helper) {
        List<String> constantColumnNames = getRawStringList(Property.COLUMN_NAMES);
        List<String> currentPKNames = helper.getStringList(KnownProperties.TARGET_PRIMARY_KEY);
        List<MigrateDataType> currentPKTypes = helper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES);

        List<String> newPKNames = new ArrayList<>();
        List<MigrateDataType> newPKTypes = new ArrayList<>();

        for (String keyName : currentPKNames) {
            if (!constantColumnNames.contains(keyName)) {
                newPKNames.add(keyName);
                newPKTypes.add(currentPKTypes.get(currentPKNames.indexOf(keyName)));
            }
            else {
                logger.info("Removing constant column {} from target PK", keyName);
            }
        }

        helper.setProperty(KnownProperties.TARGET_PRIMARY_KEY, newPKNames);
        helper.setProperty(KnownProperties.TARGET_PRIMARY_KEY_TYPES, newPKTypes);
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
