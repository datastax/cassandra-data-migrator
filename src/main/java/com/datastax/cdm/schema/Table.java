package com.datastax.cdm.schema;

import com.datastax.oss.driver.api.core.type.DataType;

import java.util.List;

public interface Table {
    String getKeyspaceName();

    String getTableName();

    List<String> getColumnNames(boolean asCql);

    List<DataType> getColumnCqlTypes();
}
