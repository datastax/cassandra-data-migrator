package com.datastax.cdm.feature;

import com.datastax.cdm.data.Record;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class Guardrail extends AbstractFeature  {
    public final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final boolean logDebug = logger.isDebugEnabled();
    private final boolean logTrace = logger.isTraceEnabled();

    public static final String CLEAN_CHECK="";
    public static final int BASE_FACTOR = 1000;
    private DecimalFormat decimalFormat = new DecimalFormat("0.###");

    private Double colSizeInKB;

    private CqlTable originTable;
    private CqlTable targetTable;

    private ExplodeMap explodeMap = null;
    private int explodeMapIndex = -1;
    private int explodeMapKeyIndex = -1;
    private int explodeMapValueIndex = -1;

    @Override
    public boolean loadProperties(IPropertyHelper propertyHelper) {
        Number property = propertyHelper.getNumber(KnownProperties.GUARDRAIL_COLSIZE_KB);
        if (null==property)
            this.colSizeInKB = 0.0;
        else
            this.colSizeInKB = property.doubleValue();

        isValid = validateProperties();
        isLoaded = true;
        isEnabled=(isValid && colSizeInKB >0);
        return isValid;
    }

    @Override
    protected boolean validateProperties() {
        isValid = true;
        if (this.colSizeInKB < 0) {
            logger.error("{} must be greater than equal to zero, but is {}", KnownProperties.GUARDRAIL_COLSIZE_KB, this.colSizeInKB);
            isValid = false;
        }

        return isValid;
    }

    @Override
    public boolean initializeAndValidate(CqlTable originTable, CqlTable targetTable) {
        if (null==originTable || !originTable.isOrigin()) {
            logger.error("originTable is null, or is not an origin table");
            return false;
        }
        if (null==targetTable || targetTable.isOrigin()) {
            logger.error("targetTable is null, or is an origin table");
            return false;
        }

        this.originTable = originTable;
        this.targetTable = targetTable;

        isValid = true;
        if (!validateProperties()) {
            isEnabled = false;
            return false;
        }

        if (logDebug) logger.debug("Guardrail is {}. colSizeInKB={}", isEnabled ? "enabled" : "disabled", colSizeInKB);

        return isValid;
    }

    private Map<String,Integer> check(Map<String,Integer> currentChecks, int targetIndex, Object targetValue) {
        int colSize = targetTable.byteCount(targetIndex, targetValue);
        if (logTrace) logger.trace("Column {} at targetIndex {} has size {} bytes", targetTable.getColumnNames(false).get(targetIndex), targetIndex, colSize);
        if (colSize > colSizeInKB * BASE_FACTOR) {
            if (null==currentChecks) currentChecks = new HashMap();
            currentChecks.put(targetTable.getColumnNames(false).get(targetIndex), colSize);
        }
        return currentChecks;
    }

    public String guardrailChecks(Record record) {
        if (!isEnabled)
            return null;
        if (null==record)
            return CLEAN_CHECK;
        if (null==record.getOriginRow())
            return CLEAN_CHECK;
        Map<String,Integer> largeColumns = null;

        // As the order of feature loading is not guaranteed, we wait until the first record to figure out the explodeMap
        if (null==explodeMap) calcExplodeMap();

        Row row = record.getOriginRow();
        for (int i=0; i<originTable.getColumnNames(false).size(); i++) {
            if (i==explodeMapIndex) {
                // Exploded columns are already converted to target type
                largeColumns = check(largeColumns, explodeMapKeyIndex, record.getPk().getExplodeMapKey());
                largeColumns = check(largeColumns, explodeMapValueIndex, record.getPk().getExplodeMapValue());
            } else {
                int targetIndex = originTable.getCorrespondingIndex(i);
                if (targetIndex<0) continue; // TTL and WRITETIME columns for example
                Object targetObject = originTable.getAndConvertData(i,row);
                largeColumns = check(largeColumns, targetIndex, targetObject);
            }
        }

        if (null==largeColumns || largeColumns.isEmpty()) return CLEAN_CHECK;

        StringBuilder sb = new StringBuilder();
        sb.append("Large columns (KB): ");
        int colCount=0;
        for (Map.Entry<String,Integer> entry : largeColumns.entrySet()) {
            if (colCount++>0) sb.append(",");
            sb.append(entry.getKey()).append("(").append(decimalFormat.format(entry.getValue()/BASE_FACTOR)).append(")");
        }

        return sb.toString();
    }

    private void calcExplodeMap() {
        this.explodeMap = (ExplodeMap) originTable.getFeature(Featureset.EXPLODE_MAP);
        if (null!=explodeMap && explodeMap.isEnabled()) {
            explodeMapIndex = explodeMap.getOriginColumnIndex();
            explodeMapKeyIndex = explodeMap.getKeyColumnIndex();
            explodeMapValueIndex = explodeMap.getValueColumnIndex();
            if (logDebug) logger.debug("ExplodeMap is enabled. explodeMapIndex={}, explodeMapKeyIndex={}, explodeMapValueIndex={}", explodeMapIndex, explodeMapKeyIndex, explodeMapValueIndex);
        }
    }
}
