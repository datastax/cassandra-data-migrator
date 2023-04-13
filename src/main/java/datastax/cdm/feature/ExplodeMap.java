package datastax.cdm.feature;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.data.PKFactory;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExplodeMap extends AbstractFeature {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public enum Property {
        MAP_COLUMN_NAME,
        MAP_COLUMN_INDEX,
        MAP_COLUMN_TYPE,
        KEY_COLUMN_NAME,
        KEY_COLUMN_TYPE,
        VALUE_COLUMN_NAME,
        VALUE_COLUMN_TYPE
    }

    private boolean valid = true;

    @Override
    public boolean initialize(PropertyHelper helper, CqlHelper cqlHelper) {
        String mapColumnName = getOriginColumnName(helper);

        putString(Property.MAP_COLUMN_NAME, mapColumnName);
        putString(Property.KEY_COLUMN_NAME, getKeyColumnName(helper));
        putString(Property.VALUE_COLUMN_NAME, getValueColumnName(helper));
        putNumber(Property.MAP_COLUMN_INDEX, getOriginColumnIndex(helper));
        putMigrateDataType(Property.MAP_COLUMN_TYPE, getOriginColumnType(helper));
        putMigrateDataType(Property.KEY_COLUMN_TYPE, getKeyColumnType(helper));
        putMigrateDataType(Property.VALUE_COLUMN_TYPE, getValueColumnType(helper));

        valid = isValid();
        isInitialized = true;
        isEnabled = valid && null!=mapColumnName && !mapColumnName.isEmpty();
        return valid;
    }

    @Override
    public PropertyHelper alterProperties(PropertyHelper helper, PKFactory pkFactory) {
            if (!valid) return null;
            if (!isEnabled) return helper;

            pkFactory.registerTypes(Arrays.asList(getRawString(Property.KEY_COLUMN_NAME),getRawString(Property.VALUE_COLUMN_NAME)),
                    Arrays.asList(getRawMigrateDataType(Property.KEY_COLUMN_TYPE),getRawMigrateDataType(Property.VALUE_COLUMN_TYPE)));
            return helper;
    }

    public static String getOriginColumnName(PropertyHelper helper) {
        String mapColumnName = helper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME);
        if (null== mapColumnName
                || mapColumnName.isEmpty()
                || !ColumnsKeysTypes.getOriginColumnNames(helper).contains(mapColumnName))
            return null;

        return mapColumnName;
    }

    public static int getOriginColumnIndex(PropertyHelper helper) {
        String mapColumnName = getOriginColumnName(helper);
        if (null== mapColumnName || mapColumnName.isEmpty()) return -1;

        List<String> originNames = ColumnsKeysTypes.getOriginColumnNames(helper);
        return originNames.indexOf(mapColumnName);
    }

    public static MigrateDataType getOriginColumnType(PropertyHelper helper) {
        int index = getOriginColumnIndex(helper);
        if (index < 0) return null;

        MigrateDataType mdt = ColumnsKeysTypes.getOriginColumnTypes(helper).get(index);
        if (mdt == null
                || mdt.getTypeClass() != Map.class)
            return null;

        return mdt;
    }

    public static String getKeyColumnName(PropertyHelper helper) {
        return helper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME);
    }

    public static MigrateDataType getKeyColumnType(PropertyHelper helper) {
        MigrateDataType mdt = getOriginColumnType(helper);
        if (mdt == null) return null;
        return mdt.getSubTypeTypes().get(0);
    }

    public static String getValueColumnName(PropertyHelper helper) {
        return helper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME);
    }

    public static MigrateDataType getValueColumnType(PropertyHelper helper) {
        MigrateDataType mdt = getOriginColumnType(helper);
        if (mdt == null) return null;
        return mdt.getSubTypeTypes().get(1);
    }

    public static MigrateDataType getExplodeMapType(PropertyHelper helper, String columnName) {
        if (null==columnName || columnName.isEmpty()) return null;

        if (columnName.equals(getKeyColumnName(helper)))
            return getKeyColumnType(helper);

        if (columnName.equals(getValueColumnName(helper)))
            return getValueColumnType(helper);

        return null;
    }

    private boolean isValid() {
        String mapColumnName = getRawString(Property.MAP_COLUMN_NAME);
        if (null== mapColumnName || mapColumnName.isEmpty()) {
            logger.error("Value is null or empty: {}",KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME);
            return false;
        }

        MigrateDataType mapDataType = getRawMigrateDataType(Property.MAP_COLUMN_TYPE);
        if (null== mapDataType) {
            logger.error("Feature requires a Map type specified at {}",KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME);
            return false;
        }

        return true;
    }

}
