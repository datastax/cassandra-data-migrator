package com.datastax.cdm.schema;

import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.oss.driver.api.core.type.DataType;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.util.List;

public class BaseTable implements Table {
    protected final IPropertyHelper propertyHelper;
    protected final boolean isOrigin;
    protected String keyspaceName;
    protected String tableName;
    protected List<String> columnNames;
    protected List<DataType> columnCqlTypes;
    protected List<CqlConversion> cqlConversions;

    public BaseTable(IPropertyHelper propertyHelper, boolean isOrigin) {
        this.propertyHelper = propertyHelper;
        this.isOrigin = isOrigin;

        String keyspaceTableString = getKeyspaceTableAsString(propertyHelper, isOrigin);
        if (keyspaceTableString.contains(".")) {
            String[] keyspaceTable = keyspaceTableString.split("\\.");
            this.keyspaceName = keyspaceTable[0];
            this.tableName = keyspaceTable[1];
        } else {
            this.keyspaceName = "";
            this.tableName = keyspaceTableString;
        }
    }

    @NotNull
    private String getKeyspaceTableAsString(IPropertyHelper propertyHelper, boolean isOrigin) {
        String keyspaceTableString = (isOrigin ? propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE) :
                propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE));

        // Use origin keyspaceTable property if target not specified
        if (!isOrigin && StringUtils.isBlank(keyspaceTableString)) {
            keyspaceTableString = propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
        }
        if (StringUtils.isBlank(keyspaceTableString)) {
            throw new RuntimeException("Value for required property " + KnownProperties.ORIGIN_KEYSPACE_TABLE + " not provided!!");
        }

        return keyspaceTableString.trim();
    }

    public String getKeyspaceName() {
        return this.keyspaceName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getKeyspaceTable() {
        return this.keyspaceName + "." + this.tableName;
    }

    public List<String> getColumnNames(boolean format) {
        return this.columnNames;
    }

    public List<DataType> getColumnCqlTypes() {
        return this.columnCqlTypes;
    }

    public List<CqlConversion> getConversions() {
        return this.cqlConversions;
    }

    public boolean isOrigin() {
        return this.isOrigin;
    }
}