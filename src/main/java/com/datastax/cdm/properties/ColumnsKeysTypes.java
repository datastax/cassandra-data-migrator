package com.datastax.cdm.properties;

import com.datastax.cdm.feature.ConstantColumns;
import com.datastax.cdm.feature.ExplodeMap;
import com.datastax.cdm.job.MigrateDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnsKeysTypes {

    public static String getOriginKeyspaceTable(PropertyHelper helper) {
        return helper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE).trim();
    }

    public static String getOriginKeyspace(PropertyHelper helper) {
        String keyspaceTable = getOriginKeyspaceTable(helper);
        if (null==keyspaceTable || keyspaceTable.isEmpty())
            return "";
        return keyspaceTable.split("\\.")[0];
    }

    public static String getOriginTable(PropertyHelper helper) {
        String keyspaceTable = getOriginKeyspaceTable(helper);
        if (null==keyspaceTable || keyspaceTable.isEmpty())
            return "";
        return keyspaceTable.split("\\.")[1];
    }

    public static String getTargetKeyspaceTable(PropertyHelper helper) {
        return helper.getString(KnownProperties.TARGET_KEYSPACE_TABLE).trim();
    }

    public static String getTargetKeyspace(PropertyHelper helper) {
        String keyspaceTable = getTargetKeyspaceTable(helper);
        if (null==keyspaceTable || keyspaceTable.isEmpty())
            return "";
        return keyspaceTable.split("\\.")[0];
    }

    public static String getTargetTable(PropertyHelper helper) {
        String keyspaceTable = getTargetKeyspaceTable(helper);
        if (null==keyspaceTable || keyspaceTable.isEmpty())
            return "";
        return keyspaceTable.split("\\.")[1];
    }

    public static Map<String,String> getOriginColumnName_TargetColumnNameMap(PropertyHelper helper) {
        Map<String,String> effectiveOriginColumnNameMap = new HashMap<>(getRawOriginColumnName_TargetColumnNameMap(helper));

        // If no configured mapping, use default mapping based on column names
        List<String> targetColumnNames = ColumnsKeysTypes.getTargetColumnNames(helper);
        for (String originColumnName : ColumnsKeysTypes.getOriginColumnNames(helper)) {
            if (targetColumnNames.contains(originColumnName) && !effectiveOriginColumnNameMap.containsKey(originColumnName))
                effectiveOriginColumnNameMap.put(originColumnName, originColumnName);
        }

        return effectiveOriginColumnNameMap;
    }

    public static Map<String,String> getTargetColumnName_OriginColumnNameMap(PropertyHelper helper) {
        // This is the inverse of getOriginColumnName_TargetColumnNameMap
        Map<String,String> effectiveTargetColumnNameMap = new HashMap<>();
        for (Map.Entry<String,String> entry : getOriginColumnName_TargetColumnNameMap(helper).entrySet()) {
            effectiveTargetColumnNameMap.put(entry.getValue(), entry.getKey());
        }
        return effectiveTargetColumnNameMap;
    }

    private static Map<String,String> getRawOriginColumnName_TargetColumnNameMap(PropertyHelper helper) {
        Map<String,String> rawOriginToTargetColumnNameMap = new HashMap<>();

        List<String> originColumnNamesToTarget = helper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET);
        List<String> originColumnNames = ColumnsKeysTypes.getOriginColumnNames(helper);

        if (null!=originColumnNamesToTarget && !originColumnNamesToTarget.isEmpty()) {
            for (String pair: originColumnNamesToTarget) {
                String[] parts = pair.split(":");
                if (parts.length!=2 || null==parts[0] || null==parts[1] ||
                        parts[0].isEmpty() || parts[1].isEmpty() ||
                        !originColumnNames.contains(parts[0])) {
                    throw new RuntimeException(KnownProperties.ORIGIN_COLUMN_NAMES_TO_TARGET + " contains invalid origin column name to target column name mapping: " + pair);
                }
                rawOriginToTargetColumnNameMap.put(parts[0], parts[1]);
            }
        }
        return rawOriginToTargetColumnNameMap;
    }

    // As target columns can be renamed, but we expect the positions of origin and target columns to be the same
    // we first look up the index on the target, then we look up the name of the column at this index on the origin
    public static List<Integer> getTargetToOriginColumnIndexes(PropertyHelper helper) {
        List<String> originColumnNames = ColumnsKeysTypes.getOriginColumnNames(helper);
        List<String> targetColumnNames = ColumnsKeysTypes.getTargetColumnNames(helper);
        Map<String,String> targetColumnNamesToOriginMap = getTargetColumnName_OriginColumnNameMap(helper);
        List<Integer> targetToOriginColumnIndexes = new ArrayList<>(targetColumnNames.size());

        // Iterate over the target column names
        for (String targetColumnName : targetColumnNames) {
            // this will be -1 if the target column name is not in the origin column names
            targetToOriginColumnIndexes.add(originColumnNames.indexOf(targetColumnNamesToOriginMap.get(targetColumnName)));
        }
        return targetToOriginColumnIndexes;
    }

    public static List<Integer> getOriginToTargetColumnIndexes(PropertyHelper helper) {
        List<String> originColumnNames = ColumnsKeysTypes.getOriginColumnNames(helper);
        List<String> targetColumnNames = ColumnsKeysTypes.getTargetColumnNames(helper);
        Map<String,String> originColumnNamesToTargetMap = getOriginColumnName_TargetColumnNameMap(helper);
        List<Integer> originToTargetColumnIndexes = new ArrayList<>(targetColumnNames.size());

        // Iterate over the origin column names
        for (String originColumnName : originColumnNames) {
            // this will be -1 if the origin column name is not in the target column names
            originToTargetColumnIndexes.add(targetColumnNames.indexOf(originColumnNamesToTargetMap.get(originColumnName)));
        }
        return originToTargetColumnIndexes;
    }


    // These must be set in config, so return them as-is
    public static List<String> getOriginColumnNames(PropertyHelper helper) {
        List<String> currentColumnNames = helper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES);
        return currentColumnNames;
    }

    // These must be set in config, so return them as-is
    public static List<MigrateDataType> getOriginColumnTypes(PropertyHelper helper) {
        List<MigrateDataType> currentColumnTypes = helper.getMigrationTypeList(KnownProperties.ORIGIN_COLUMN_TYPES);
        return currentColumnTypes;
    }

    public static List<String> getOriginPartitionKeyNames(PropertyHelper helper) {
        List<String> originPartitionKeyNames = helper.getStringList(KnownProperties.ORIGIN_PARTITION_KEY);
        return originPartitionKeyNames;
    }

    // These must be set in config, so return them as-is
    public static List<String> getTargetPKNames(PropertyHelper helper) {
        List<String> currentPKNames = helper.getStringList(KnownProperties.TARGET_PRIMARY_KEY);
        return currentPKNames;
    }

    public static List<String> getOriginPKNames(PropertyHelper helper) {
        List<String> currentPKNames = helper.getStringList(KnownProperties.ORIGIN_PRIMARY_KEY_NAMES);
        if (null==currentPKNames || currentPKNames.isEmpty()) {
            Map<String,String> targetToOriginNameMap = getTargetColumnName_OriginColumnNameMap(helper);
            currentPKNames = new ArrayList<>();
            for (String targetPKName : getTargetPKNames(helper)) {
                String originPKName = targetToOriginNameMap.get(targetPKName);
                if (null!=originPKName)
                    currentPKNames.add(targetToOriginNameMap.get(targetPKName));
            }
        }
        return currentPKNames;
    }

    public static List<MigrateDataType> getOriginPKTypes(PropertyHelper helper) {
        List<MigrateDataType> currentPKTypes = helper.getMigrationTypeList(KnownProperties.ORIGIN_PRIMARY_KEY_TYPES);
        if (null==currentPKTypes || currentPKTypes.isEmpty()) {
            currentPKTypes = new ArrayList<>();
            Map<String,MigrateDataType> originColumnNameToTypeMap = getOriginColumnNameToTypeMap(helper);
            for (String originPKName : getOriginPKNames(helper)) {
                MigrateDataType type = originColumnNameToTypeMap.get(originPKName);
                if (null!=type)
                    currentPKTypes.add(originColumnNameToTypeMap.get(originPKName));
            }
        }
        return currentPKTypes;
    }

    public static Map<String,MigrateDataType> getOriginColumnNameToTypeMap(PropertyHelper helper) {
        Map<String,MigrateDataType> columnNameToTypeMap = new HashMap<>();
        List<String> originColumnNames = getOriginColumnNames(helper);
        List<MigrateDataType> originColumnTypes = getOriginColumnTypes(helper);
        if (null!=originColumnNames && null!=originColumnTypes && originColumnNames.size() == originColumnTypes.size()) {
            for (int i=0; i<originColumnNames.size(); i++) {
                columnNameToTypeMap.put(originColumnNames.get(i), originColumnTypes.get(i));
            }
        }
        else
            throw new RuntimeException(KnownProperties.ORIGIN_COLUMN_NAMES + " and " + KnownProperties.ORIGIN_COLUMN_TYPES + " must be the same size and not null");
        return columnNameToTypeMap;
    }


    public static List<String> getTargetColumnNames(PropertyHelper helper) {
        List<String> currentColumnNames = helper.getStringList(KnownProperties.TARGET_COLUMN_NAMES);

        // Default to the origin column names if the target column names are not set
        // But first consult the renaming map to see if any of the origin column names have been renamed
        // and use the renamed values if they have been.
        if (null==currentColumnNames || currentColumnNames.isEmpty()) {
            Map<String,String> originToTargetNameMap = getRawOriginColumnName_TargetColumnNameMap(helper);

            currentColumnNames = new ArrayList<>(getOriginColumnNames(helper).size());
            for (String originColumnName : getOriginColumnNames(helper)) {
                String targetColumnName = originToTargetNameMap.get(originColumnName);
                if (null==targetColumnName)
                    targetColumnName = originColumnName;
                currentColumnNames.add(targetColumnName);
            }
        }

        List<String> returnedColumnNames = new ArrayList<>(currentColumnNames.size());

        // If any feature-related columns have been specified, remove them from the list as we will
        // add them in specific positions
        for (String columnName : currentColumnNames) {
            // If it is a feature-specific column, skip it
            if (null!=getFeatureType(helper, columnName))
                continue;

            // If this is the explode map origin column, it does not belong on the target
            if (columnName.equals(ExplodeMap.getOriginColumnName(helper)))
                continue;

            returnedColumnNames.add(columnName);
        }

        String explodeMapKeyName = ExplodeMap.getKeyColumnName(helper);
        if (null!=explodeMapKeyName && !explodeMapKeyName.isEmpty())
            returnedColumnNames.add(explodeMapKeyName);

        String explodeMapValueName = ExplodeMap.getValueColumnName(helper);
        if (null!=explodeMapValueName && !explodeMapKeyName.isEmpty())
            returnedColumnNames.add(explodeMapValueName);

        // Constant columns at end of list as they are not to be bound
        List<String> constantColumnNames = ConstantColumns.getConstantColumnNames(helper);
        if (null!=constantColumnNames && !constantColumnNames.isEmpty()) {
            for (String constantColumnName : constantColumnNames) {
                if (!returnedColumnNames.contains(constantColumnName))
                    returnedColumnNames.add(constantColumnName);
            }
        }

        return returnedColumnNames;
    }

    public static List<MigrateDataType> getTargetColumnTypes(PropertyHelper helper) {
        List<MigrateDataType> currentColumnTypes = helper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES);
        if (null==currentColumnTypes || currentColumnTypes.isEmpty() || currentColumnTypes.size() != getTargetColumnNames(helper).size()) {

            currentColumnTypes = new ArrayList<>();

            // order should match getTargetColumnNames()
            for (String targetColumnName : getTargetColumnNames(helper)) {
                String originColumnName = getTargetColumnName_OriginColumnNameMap(helper).get(targetColumnName);
                MigrateDataType originDataType = null;

                // Try to get the value from the origin list
                if (null!=originColumnName)
                    originDataType = getOriginColumnNameToTypeMap(helper).get(originColumnName);

                if (null != originDataType) {
                    // If that matches, we use it
                    currentColumnTypes.add(originDataType);
                    continue;
                }
                else {
                    // otherwise, search features to see if they can tell us the type
                    MigrateDataType t = getFeatureType(helper,targetColumnName);
                    if (null!=t) {
                        currentColumnTypes.add(t);
                        continue;
                    }
                }

                // We do not know what this column type is, set to unknown type
                currentColumnTypes.add(new MigrateDataType());
            }
        }
        return currentColumnTypes;
    }

    public static Map<String,MigrateDataType> getTargetColumnNameToTypeMap(PropertyHelper helper) {
        Map<String,MigrateDataType> columnNameToTypeMap = new HashMap<>();
        List<String> targetColumnNames = getTargetColumnNames(helper);
        List<MigrateDataType> targetColumnTypes = getTargetColumnTypes(helper);
        if (null!=targetColumnNames && null!=targetColumnTypes && targetColumnNames.size() == targetColumnTypes.size()) {
            for (int i=0; i<targetColumnNames.size(); i++) {
                columnNameToTypeMap.put(targetColumnNames.get(i), targetColumnTypes.get(i));
            }
        }
        else {
            throw new RuntimeException(String.format("Target column names %s and types %s must be the same size and not null", targetColumnNames, targetColumnTypes));
        }
        return columnNameToTypeMap;
    }

    public static List<MigrateDataType> getTargetPKTypes(PropertyHelper helper) {
        List<String> targetPKNames = getTargetPKNames(helper);
        Map<String,MigrateDataType> targetColumnNameToTypeMap = getTargetColumnNameToTypeMap(helper);

        List<MigrateDataType> currentPKTypes = new ArrayList<>();
        for (String targetPKName : targetPKNames) {
            MigrateDataType targetPKType = targetColumnNameToTypeMap.get(targetPKName);
            currentPKTypes.add(targetPKType);
        }

        return currentPKTypes;
    }

    public static MigrateDataType getFeatureType(PropertyHelper helper, String columnName) {
        MigrateDataType constantColumnType = ConstantColumns.getConstantColumnType(helper, columnName);
        if (null!=constantColumnType)
            return constantColumnType;

        MigrateDataType explodeMapType = ExplodeMap.getExplodeMapType(helper, columnName);
        if (null!=explodeMapType)
            return explodeMapType;

        return null;
    }

}
