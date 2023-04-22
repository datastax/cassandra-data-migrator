package com.datastax.cdm.cql;

import com.datastax.cdm.cql.statement.*;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.cdm.cql.codec.CodecFactory;
import com.datastax.cdm.cql.codec.Codecset;
import com.datastax.cdm.data.PKFactory;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EnhancedSession {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    boolean logDebug = logger.isDebugEnabled();

    private final PropertyHelper propertyHelper;
    private final CqlSession cqlSession;
    private final CqlTable cqlTable;
    private final boolean isOrigin;
    private PKFactory pkFactory;

    public EnhancedSession(PropertyHelper propertyHelper, CqlSession cqlSession, boolean isOrigin) {
        this.propertyHelper = propertyHelper;
        this.cqlSession = initSession(propertyHelper, cqlSession);
        this.isOrigin = isOrigin;

        cqlTable = new CqlTable(propertyHelper, isOrigin, cqlSession);
    }

    public void setPKFactory(PKFactory pkFactory) {
        this.pkFactory = pkFactory;
    }
    public PKFactory getPKFactory() { return this.pkFactory; }

    public CqlSession getCqlSession() {
        return cqlSession;
    }
    public CqlTable getCqlTable() {
        return cqlTable;
    }

    public OriginSelectByPartitionRangeStatement getOriginSelectByPartitionRangeStatement() {
        if (!isOrigin) throw new RuntimeException("This is not an origin session");
        return new OriginSelectByPartitionRangeStatement(propertyHelper, this);
    }

    public OriginSelectByPKStatement getOriginSelectByPKStatement() {
        if (!isOrigin) throw new RuntimeException("This is not an origin session");
        return new OriginSelectByPKStatement(propertyHelper, this);
    }

    public TargetSelectByPKStatement getTargetSelectByPKStatement() {
        if (isOrigin) throw new RuntimeException("This is not a target session");
        return new TargetSelectByPKStatement(propertyHelper, this);
    }

    public TargetUpsertStatement getTargetUpsertStatement() {
        if (isOrigin) throw new RuntimeException("This is not a target session");
        if (cqlTable.isCounterTable())
            return new TargetUpdateStatement(propertyHelper, this);
        else
            return new TargetInsertStatement(propertyHelper, this);
    }

    private CqlSession initSession(PropertyHelper propertyHelper, CqlSession session) {
        List<String> codecList = propertyHelper.getStringList(KnownProperties.TRANSFORM_CODECS);
        if (null!=codecList && !codecList.isEmpty()) {
            MutableCodecRegistry registry = (MutableCodecRegistry) session.getContext().getCodecRegistry();

            for (String codecString : codecList) {
                Codecset codecEnum = Codecset.valueOf(codecString);
                for (TypeCodec<?> codec : CodecFactory.getCodecPair(propertyHelper, codecEnum)) {
                    DataType dataType = codec.getCqlType();
                    GenericType<?> javaType = codec.getJavaType();
                    if (logDebug) logger.debug("Registering Codec {} for CQL type {} and Java type {}", codec.getClass().getSimpleName(), dataType, javaType);
                    try {
                        TypeCodec<?> existingCodec = registry.codecFor(dataType, javaType);
                    } catch (CodecNotFoundException e) {
                        registry.register(codec);
                    }
                }
            }
        }
        return session;
    }



}
