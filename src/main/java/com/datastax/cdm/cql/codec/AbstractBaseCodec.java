package com.datastax.cdm.cql.codec;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.cdm.cql.CqlHelper;
import com.datastax.cdm.properties.PropertyHelper;

public abstract class AbstractBaseCodec<T> implements TypeCodec<T> {
    private final PropertyHelper propertyHelper;
    private final CqlHelper cqlHelper;

    public AbstractBaseCodec(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        this.propertyHelper = propertyHelper;
        this.cqlHelper = cqlHelper;
    }

}