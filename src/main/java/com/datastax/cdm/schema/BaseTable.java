package com.datastax.cdm.schema;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.cdm.data.CqlConversion;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;

import java.util.List;

public class BaseTable implements Table {
    protected final PropertyHelper propertyHelper;
    protected final boolean isOrigin;

    protected String keyspaceName;
    protected String tableName;
    protected List<String> columnNames;
    protected List<DataType> columnCqlTypes;
    protected List<CqlConversion> cqlConversions;

    public BaseTable(PropertyHelper propertyHelper, boolean isOrigin) {
        this.propertyHelper = propertyHelper;
        this.isOrigin = isOrigin;

        String keyspaceTableString = (this.isOrigin ? propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE) : propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE)).trim();
        if (keyspaceTableString.contains(".")) {
            String[] keyspaceTable = keyspaceTableString.split("\\.");
            this.keyspaceName = keyspaceTable[0];
            this.tableName = keyspaceTable[1];
        } else {
            this.keyspaceName = "";
            this.tableName = keyspaceTableString;
        }
    }

    public String getKeyspaceName() {return this.keyspaceName;}
    public String getTableName() {return this.tableName;}
    public String getKeyspaceTable() {return this.keyspaceName + "." + this.tableName;}
    public List<String> getColumnNames(boolean format) { return this.columnNames; }
    public List<DataType> getColumnCqlTypes() {return this.columnCqlTypes;}
    public List<CqlConversion> getConversions() {return this.cqlConversions;}
    public boolean isOrigin() {return this.isOrigin;}
}
