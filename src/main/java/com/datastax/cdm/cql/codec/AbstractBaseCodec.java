package com.datastax.cdm.cql.codec;

import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

public abstract class AbstractBaseCodec<T> implements TypeCodec<T> {
    private final PropertyHelper propertyHelper;

    public AbstractBaseCodec(PropertyHelper propertyHelper) {
        this.propertyHelper = propertyHelper;
    }

}