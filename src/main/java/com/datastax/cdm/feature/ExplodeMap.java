package com.datastax.cdm.feature;

import com.datastax.cdm.data.CqlData;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExplodeMap extends AbstractFeature {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private String originColumnName = "";
    private Integer originColumnIndex = -1;

    private String keyColumnName = "";
    private Integer keyColumnIndex = -1;

    private String valueColumnName = "";
    private Integer valueColumnIndex = -1;

    @Override
    public boolean loadProperties(IPropertyHelper helper) {
        if (null == helper) { throw new IllegalArgumentException("helper is null");}

        this.originColumnName = getOriginColumnName(helper);
        this.keyColumnName = getKeyColumnName(helper);
        this.valueColumnName = getValueColumnName(helper);

        isValid = validateProperties();

        isEnabled = isValid
               && !originColumnName.isEmpty()
               && !keyColumnName.isEmpty()
               && !valueColumnName.isEmpty();

        isLoaded = true;
        return isLoaded && isValid;
    }

    @Override
    protected boolean validateProperties() {
        isValid = true;
        if ((null == originColumnName || originColumnName.isEmpty()) &&
            (null == keyColumnName || keyColumnName.isEmpty()) &&
            (null == valueColumnName || valueColumnName.isEmpty()))
            return true;

        if (null==originColumnName || originColumnName.isEmpty()) {
            logger.error("Origin column name is not set when Key ({}) and/or Value ({}) are set", keyColumnName, valueColumnName);
            isValid = false;
        }

        if (null==keyColumnName || keyColumnName.isEmpty()) {
            logger.error("Key column name is not set when Origin ({}) and/or Value ({}) are set", originColumnName, valueColumnName);
            isValid = false;
        }

        if (null==valueColumnName || valueColumnName.isEmpty()) {
            logger.error("Value column name is not set when Origin ({}) and/or Key ({}) are set", originColumnName, keyColumnName);
            isValid = false;
        }

        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null==originTable || null==targetTable) {
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
        if (!isEnabled) return true;

        // Initialize Origin variables
        List<Class> originBindClasses = originTable.extendColumns(Collections.singletonList(originColumnName));
        if (null == originBindClasses || originBindClasses.size() != 1 || null == originBindClasses.get(0)) {
            logger.error("Origin column {} is not found on the origin table {}", originColumnName, originTable.getKeyspaceTable());
            isValid = false;
        } else {
            if (!CqlData.Type.MAP.equals(CqlData.toType(originTable.getDataType(originColumnName)))) {
                logger.error("Origin column {} is not a map, it is {}", originColumnName, originBindClasses.get(0).getName());
                isValid = false;
            } else {
                this.originColumnIndex = originTable.indexOf(originColumnName);
            }
        }

        // Initialize Target variables
        List<Class> targetBindClasses = targetTable.extendColumns(Arrays.asList(keyColumnName, valueColumnName));
        if (null == targetBindClasses || targetBindClasses.size() != 2 || null == targetBindClasses.get(0) || null == targetBindClasses.get(1)) {
            if (null == targetBindClasses.get(0))
                logger.error("Target key column {} is not found on the target table {}", keyColumnName, targetTable.getKeyspaceTable());
            if (null == targetBindClasses.get(1))
                logger.error("Target value column {} is not found on the target table {}", valueColumnName, targetTable.getKeyspaceTable());
            isValid = false;
        } else {
            this.keyColumnIndex = targetTable.indexOf(keyColumnName);
            this.valueColumnIndex = targetTable.indexOf(valueColumnName);
        }

        if (!isValid) isEnabled = false;
        logger.info("Feature {} is {}", this.getClass().getSimpleName(), isEnabled?"enabled":"disabled");
        return isValid;
    }

    public String getOriginColumnName() { return isEnabled ? originColumnName : ""; }
    public Integer getOriginColumnIndex() { return isEnabled ? originColumnIndex : -1; }

    public String getKeyColumnName() { return isEnabled ? keyColumnName : ""; }
    public Integer getKeyColumnIndex() { return isEnabled ? keyColumnIndex : -1; }

    public String getValueColumnName() { return isEnabled ? valueColumnName : ""; }
    public Integer getValueColumnIndex() { return isEnabled ? valueColumnIndex : -1; }

    public static String getOriginColumnName(IPropertyHelper helper) {
        if (null == helper) { throw new IllegalArgumentException("helper is null");}
        String columnName = CqlTable.unFormatName(helper.getString(KnownProperties.EXPLODE_MAP_ORIGIN_COLUMN_NAME));
        return (null == columnName) ? "" : columnName;
    }

    public static String getKeyColumnName(IPropertyHelper helper) {
        if (null == helper) { throw new IllegalArgumentException("helper is null");}
        String columnName = CqlTable.unFormatName(helper.getString(KnownProperties.EXPLODE_MAP_TARGET_KEY_COLUMN_NAME));
        return (null == columnName) ? "" : columnName;
    }

    public static String getValueColumnName(IPropertyHelper helper) {
        if (null == helper) { throw new IllegalArgumentException("helper is null");}
        String columnName = CqlTable.unFormatName(helper.getString(KnownProperties.EXPLODE_MAP_TARGET_VALUE_COLUMN_NAME));
        return (null == columnName) ? "" : columnName;
    }

}
