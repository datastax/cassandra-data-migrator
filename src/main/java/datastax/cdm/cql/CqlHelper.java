package datastax.cdm.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import datastax.cdm.cql.codec.CodecFactory;
import datastax.cdm.cql.codec.Codecset;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.data.PKFactory;
import datastax.cdm.feature.Feature;
import datastax.cdm.feature.FeatureFactory;
import datastax.cdm.feature.Featureset;
import datastax.cdm.cql.statement.*;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CqlHelper {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private CqlSession originSession;
    private CqlSession targetSession;

    private ConsistencyLevel readConsistencyLevel;
    private ConsistencyLevel writeConsistencyLevel;

    public final PropertyHelper propertyHelper;
    private final Map<Featureset, Feature> featureMap = new HashMap<>(Featureset.values().length);
    private PKFactory pkFactory;
    private OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement;
    private OriginSelectByPKStatement originSelectByPKStatement;
    private TargetInsertStatement targetInsertStatement;
    private TargetUpdateStatement targetUpdateStatement;
    private TargetSelectByPKStatement targetSelectByPKStatement;

    private final Map<Codecset, TypeCodec<?>> codecMap = new HashMap<>(Codecset.values().length);

    // Constructor
    public CqlHelper() {
        this.propertyHelper = PropertyHelper.getInstance();
    }

    public boolean initialize() {
        boolean validInit = true;

        readConsistencyLevel = mapToConsistencyLevel(propertyHelper.getString(KnownProperties.READ_CL));
        writeConsistencyLevel = mapToConsistencyLevel(propertyHelper.getString(KnownProperties.WRITE_CL));;

        for (Featureset f : Featureset.values()) {
            if (f.toString().startsWith("TEST_")) continue; // Skip test features
            Feature feature = FeatureFactory.getFeature(f); // FeatureFactory throws an RTE if the feature is not implemented
            if (!feature.initialize(this.propertyHelper))
                validInit = false;
            else
                featureMap.put(f, feature);
        }

        pkFactory = new PKFactory(propertyHelper, this);

        for (Featureset f : Featureset.values()) {
            if (f.toString().startsWith("TEST_")) continue; // Skip test features
            Feature feature = getFeature(f);
            if (isFeatureEnabled(f))
                feature.alterProperties(this.propertyHelper, this.pkFactory);
        }

        registerTargetCodecs();

        originSelectByPartitionRangeStatement = new OriginSelectByPartitionRangeStatement(propertyHelper,this);
        originSelectByPKStatement = new OriginSelectByPKStatement(propertyHelper,this);
        targetInsertStatement = new TargetInsertStatement(propertyHelper,this);
        targetUpdateStatement = new TargetUpdateStatement(propertyHelper,this);
        targetSelectByPKStatement = new TargetSelectByPKStatement(propertyHelper,this);

        logger.info("PARAM -- Read Consistency: {}", readConsistencyLevel);
        logger.info("PARAM -- Write Consistency: {}", writeConsistencyLevel);
        logger.info("PARAM -- Write Batch Size: {}", getBatchSize());
        logger.info("PARAM -- Read Fetch Size: {}", getFetchSizeInRows());
        logger.info("PARAM -- Origin Keyspace Table: {}", getOriginKeyspaceTable());
        logger.info("PARAM -- Target Keyspace Table: {}", getTargetKeyspaceTable());
        logger.info("PARAM -- TTLCols: {}", getTtlCols());
        logger.info("PARAM -- WriteTimestampCols: {}", getWriteTimeStampCols());
        logger.info("PARAM -- ORIGIN SELECT Query used: {}", originSelectByPartitionRangeStatement.getCQL());
        logger.info("PARAM -- TARGET INSERT Query used: {}", targetInsertStatement.getCQL());
        logger.info("PARAM -- TARGET UPDATE Query used: {}", targetUpdateStatement.getCQL());
        logger.info("PARAM -- TARGET SELECT Query used: {}", targetSelectByPKStatement.getCQL());

        return validInit;
    }

    // ----------------- Core Feature and CQL methods --------------
    public Feature getFeature(Featureset featureEnum) {
        return featureMap.get(featureEnum);
    }

    public Boolean isFeatureEnabled(Featureset featureEnum) {
        if (!featureMap.containsKey(featureEnum)) {
            return false;
        }
        return featureMap.get(featureEnum).isEnabled();
    }

    public PKFactory getPKFactory() {return pkFactory;}
    public OriginSelectByPartitionRangeStatement getOriginSelectByPartitionRangeStatement() {return originSelectByPartitionRangeStatement;}
    public OriginSelectByPKStatement getOriginSelectByPKStatement() {return originSelectByPKStatement;}
    public TargetInsertStatement getTargetInsertStatement() {return targetInsertStatement;}
    public TargetUpdateStatement getTargetUpdateStatement() {return targetUpdateStatement;}
    public TargetSelectByPKStatement getTargetSelectByPKStatement() {return targetSelectByPKStatement;}

    // ----------------- Codec Functions --------------
    private void registerTargetCodecs() {
        List<String> codecList = propertyHelper.getStringList(KnownProperties.TRANSFORM_CODECS);
        if (null!=codecList && !codecList.isEmpty()) {
            MutableCodecRegistry registry =
                    (MutableCodecRegistry) targetSession.getContext().getCodecRegistry();

            StringBuilder sb = new StringBuilder("PARAM -- Codecs Enabled: ");
            for (String codecString : codecList) {
                Codecset codecEnum = Codecset.valueOf(codecString);
                for (TypeCodec<?> codec : CodecFactory.getCodecs(propertyHelper, this, codecEnum)) {
                    registry.register(codec);
                    codecMap.put(codecEnum, codec);
                }
                sb.append(codecString).append(" ");
            }
            logger.info(sb.toString());
        }
    }
    public boolean isCodecRegistered(Codecset codecEnum) {
        return codecMap.containsKey(codecEnum);
    }

    // --------------- Session and Performance -------------------------
    public void setOriginSession(CqlSession originSession) {
        this.originSession = originSession;
    }

    public void setTargetSession(CqlSession targetSession) {
        this.targetSession = targetSession;
    }

    public CqlSession getOriginSession() {
        return originSession;
    }

    public CqlSession getTargetSession() {
        return targetSession;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }
    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public Integer getFetchSizeInRows() {
        return propertyHelper.getInteger(KnownProperties.PERF_FETCH_SIZE);
    }

    public Integer getBatchSize() {
        // cannot do batching if the writeFilter is greater than 0 or maxWriteTimeStampFilter is less than max long
        // do not batch for counters as it adds latency & increases chance of discrepancy
        if (originSelectByPartitionRangeStatement.hasWriteTimestampFilter() || isCounterTable())
            return 1;
        else {
            Integer rtn = propertyHelper.getInteger(KnownProperties.PERF_BATCH_SIZE);
            return (null==rtn || rtn < 1) ? 5 : rtn;
        }
    }

    // -------------- Schema ----------------------
    private String getOriginKeyspaceTable() {
        return propertyHelper.getString(KnownProperties.ORIGIN_KEYSPACE_TABLE);
    }


    private String getTargetKeyspaceTable() {
        return propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE);
    }


    public boolean hasRandomPartitioner() {
        return propertyHelper.getBoolean(KnownProperties.ORIGIN_HAS_RANDOM_PARTITIONER);
    }

    public boolean isCounterTable() {
        List<Integer> rtn = propertyHelper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEXES);
        return (null != rtn && rtn.size() > 0);
    }

    //--------------- TTL & Writetime Feature ---------------
    public List<Integer> getTtlCols() {
        return propertyHelper.getIntegerList(KnownProperties.ORIGIN_TTL_INDEXES);
    }

    public List<Integer> getWriteTimeStampCols() {
        return propertyHelper.getIntegerList(KnownProperties.ORIGIN_WRITETIME_INDEXES);
    }

    //----------- General Utilities --------------
    public Object getData(MigrateDataType dataType, int index, Row row) {
        if (dataType.getTypeClass() == Map.class) {
            return row.getMap(index, dataType.getSubTypeClasses().get(0), dataType.getSubTypeClasses().get(1));
        } else if (dataType.getTypeClass() == List.class) {
            return row.getList(index, dataType.getSubTypeClasses().get(0));
        } else if (dataType.getTypeClass() == Set.class) {
            return row.getSet(index, dataType.getSubTypeClasses().get(0));
        } else if (isCounterTable() && dataType.getTypeClass() == Long.class) {
            Object data = row.get(index, dataType.getTypeClass());
            if (data == null) {
                return new Long(0);
            }
        }

        return row.get(index, dataType.getTypeClass());
    }

    private static ConsistencyLevel mapToConsistencyLevel(String level) {
        ConsistencyLevel retVal = ConsistencyLevel.LOCAL_QUORUM;
        if (StringUtils.isNotEmpty(level)) {
            switch (level.toUpperCase()) {
                case "ANY":
                    retVal = ConsistencyLevel.ANY;
                    break;
                case "ONE":
                    retVal = ConsistencyLevel.ONE;
                    break;
                case "TWO":
                    retVal = ConsistencyLevel.TWO;
                    break;
                case "THREE":
                    retVal = ConsistencyLevel.THREE;
                    break;
                case "QUORUM":
                    retVal = ConsistencyLevel.QUORUM;
                    break;
                case "LOCAL_ONE":
                    retVal = ConsistencyLevel.LOCAL_ONE;
                    break;
                case "EACH_QUORUM":
                    retVal = ConsistencyLevel.EACH_QUORUM;
                    break;
                case "SERIAL":
                    retVal = ConsistencyLevel.SERIAL;
                    break;
                case "LOCAL_SERIAL":
                    retVal = ConsistencyLevel.LOCAL_SERIAL;
                    break;
                case "ALL":
                    retVal = ConsistencyLevel.ALL;
                    break;
            }
        }

        return retVal;
    }

}
