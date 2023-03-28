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
        COLUMN_VALUES
    }

    public enum Function {
        TARGET_PK_WITHOUT_CONSTANTS,
        TEST_FUNCTION
    }

    private boolean valid = true;

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

        isInitialized = true;
        valid = isValid(propertyHelper);
        isEnabled = valid && null!=columnNames && !columnNames.isEmpty();
        return valid;
    }

    @Override
    public Object featureFunction(Enum<?> function, Object... args) {
        switch ((Function) function) {
            case TARGET_PK_WITHOUT_CONSTANTS:
                // args[] should be List<MigrateDataType> targetPrimaryKeyTypes, List<String> targetPrimaryKeyNames
                if (null==args || args.length!=2 || null==args[0] || null==args[1])
                    throw new IllegalArgumentException("Expected 2 not-null arguments, got " + (null==args ? "1" : args.length));
                if (!(args[0] instanceof List<?>) || ((List<?>) args[0]).isEmpty() || !(((List<?>) args[0]).get(0) instanceof MigrateDataType))
                    throw new IllegalArgumentException("First argument should be a non-empty List<MigrateDataType>, got " + args[0]);
                if (!(args[1] instanceof List<?>) || ((List<?>) args[1]).isEmpty() || !(((List<?>) args[1]).get(0) instanceof String))
                    throw new IllegalArgumentException("Second argument should be a non-empty List<String>, got " + args[1]);
                return targetPrimaryKeyTypesWithoutConstantColumns((List<MigrateDataType>)args[0], (List<String>)args[1]);
        }
        return null;
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

    private List<MigrateDataType> targetPrimaryKeyTypesWithoutConstantColumns(List<MigrateDataType> targetPrimaryKeyTypes, List<String> targetPrimaryKeyNames) {
        if (!isEnabled) return targetPrimaryKeyTypes;
        if (!valid) return null;

        // As this is valid, we know that the column names, types, and values are all the same size
        List<String> columnNames = getRawStringList(Property.COLUMN_NAMES);

        List<MigrateDataType> rtn = new ArrayList<>();
        for (String keyName : targetPrimaryKeyNames) {
            if (!columnNames.contains(keyName)) {
                rtn.add(targetPrimaryKeyTypes.get(targetPrimaryKeyNames.indexOf(keyName)));
            }
        }
        return rtn;
    }

    private boolean isValid(PropertyHelper propertyHelper) {
        List<String> columnNames = getRawStringList(Property.COLUMN_NAMES);
        List<MigrateDataType> columnTypes = getRawMigrateDataTypeList(Property.COLUMN_TYPES);
        List<String> columnValues = getRawStringList(Property.COLUMN_VALUES);

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

        return valid;
    }
}
