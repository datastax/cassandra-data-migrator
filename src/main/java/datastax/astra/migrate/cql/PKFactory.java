package datastax.astra.migrate.cql;

import com.datastax.oss.driver.api.core.cql.Row;
import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.cql.features.*;
import datastax.astra.migrate.cql.statements.OriginSelectByPartitionRangeStatement;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PKFactory {
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private enum LookupMethod {
        ORIGIN_COLUMN,
        CONSTANT_COLUMN,
        EXPLODE_MAP
    }

    public enum Side {
        ORIGIN,
        TARGET
    }

    private final CqlHelper cqlHelper;

    private final List<String> targetPKNames = new ArrayList<>();
    private final List<MigrateDataType> targetPKTypes = new ArrayList<>();
    private final List<Integer> targetPKIndexesToBind;
    private final List<LookupMethod> targetPKLookupMethods;
    private final List<Object> targetDefaultValues;
    private final String targetWhereClause;

    private final List<MigrateDataType> targetColumnTypes;
    private final List<MigrateDataType> originColumnTypes;
    private final List<Integer> targetToOriginColumnIndexes;
    private final List<Integer> targetToOriginPKIndexes;

    private final List<String> originPKNames = new ArrayList<>();
    private final List<MigrateDataType> originPKTypes = new ArrayList<>();
    private final List<Integer> originPKIndexesToBind;
    private final List<LookupMethod> originPKLookupMethods;
    private final String originWhereClause;

    private final Integer explodeMapOriginColumnIndex;
    private final Integer explodeMapTargetKeyColumnIndex;
    private final Integer explodeMapTargetPKIndex;

    // These defaults address the problem where we cannot insert null values into a PK column
    private final Long defaultForMissingTimestamp;
    private final String defaultForMissingString;

    private OriginSelectByPartitionRangeStatement originSelectByPartitionRangeStatement;

    public PKFactory(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        this.cqlHelper = cqlHelper;

        setPKNamesAndTypes(propertyHelper);

        this.targetColumnTypes = propertyHelper.getMigrationTypeList(KnownProperties.TARGET_COLUMN_TYPES);
        this.originColumnTypes = propertyHelper.getMigrationTypeList(KnownProperties.ORIGIN_COLUMN_TYPES);
        this.targetToOriginColumnIndexes = targetToOriginColumnIndexes(propertyHelper);

        this.targetPKLookupMethods = new ArrayList<>(targetPKNames.size());
        this.targetDefaultValues = new ArrayList<>(targetPKNames.size());
        for (int i = 0; i< targetPKNames.size(); i++) {
            targetPKLookupMethods.add(null);
            targetDefaultValues.add(null);
        }

        this.originPKLookupMethods = new ArrayList<>(originPKNames.size());
        for (int i = 0; i< originPKNames.size(); i++) {
            originPKLookupMethods.add(null);
        }

        this.defaultForMissingTimestamp = propertyHelper.getLong(KnownProperties.TARGET_REPLACE_MISSING_TS);
        this.defaultForMissingString = "";

        setOriginColumnLookupMethod(propertyHelper);
        setConstantColumns();
        this.explodeMapTargetKeyColumnIndex = setExplodeMapMethods_getTargetKeyColumnIndex();
        this.explodeMapOriginColumnIndex = getExplodeMapOriginColumnIndex();
        this.explodeMapTargetPKIndex = targetPKLookupMethods.indexOf(LookupMethod.EXPLODE_MAP);

        // These need to be set once all the features have been processed
        scrubLookupMethods();
        this.targetToOriginPKIndexes = targetToOriginPKIndexes(propertyHelper);
        this.targetPKIndexesToBind = getIndexesToBind(Side.TARGET);
        this.originPKIndexesToBind = getIndexesToBind(Side.ORIGIN);
        this.targetWhereClause = getWhereClause(Side.TARGET);
        this.originWhereClause = getWhereClause(Side.ORIGIN);

        if (targetPKTypes.size() != targetPKLookupMethods.size()) {
            throw new RuntimeException("Unable to locate a method to determine value of each primary key column");
        }
    }

    public EnhancedPK getTargetPK(Row originRow) {
        List<Object> newValues = getTargetPKValuesFromOriginColumnLookupMethod(originRow, targetDefaultValues);
        Long originWriteTimeStamp = getOriginSelectByPartitionRangeStatement().getLargestWriteTimeStamp(originRow);
        Long originTTL = getOriginSelectByPartitionRangeStatement().getLargestTTL(originRow);
        if (explodeMapTargetKeyColumnIndex < 0) {
            return new EnhancedPK(this, newValues, getPKTypes(Side.TARGET), originWriteTimeStamp, originTTL);
        }
        else {
            Map<Object, Object> explodeMap = getExplodeMap(originRow);
            return new EnhancedPK(this, newValues, getPKTypes(Side.TARGET), originWriteTimeStamp, originTTL, explodeMap);
        }
    }

    public EnhancedPK toEnhancedPK(List<Object> pkValues, List<MigrateDataType> pkTypes) {
        return new EnhancedPK(this, pkValues, pkTypes, null, null, null);
    }

    public String getWhereClause(Side side) {
        StringBuilder sb;
        switch (side) {
            case ORIGIN:
                if (null!=originWhereClause && !originWhereClause.isEmpty()) return originWhereClause;
                sb = new StringBuilder();
                for (int i=0; i<originPKNames.size(); i++) {
                    LookupMethod method = originPKLookupMethods.get(i);
                    String name = originPKNames.get(i);

                    // On origin PK, we don't bind anything other than ORIGIN_COLUMN
                    if (method == LookupMethod.ORIGIN_COLUMN) {
                        if (sb.length() > 0) sb.append(" AND ");
                        sb.append(name).append("=?");
                    }
                }
                return sb.toString();
            case TARGET:
                if (null!=targetWhereClause && !targetWhereClause.isEmpty()) return targetWhereClause;
                sb = new StringBuilder();
                for (int i=0; i<targetPKNames.size(); i++) {
                    LookupMethod method = targetPKLookupMethods.get(i);
                    String name = targetPKNames.get(i);
                    Object defaultValue = targetDefaultValues.get(i);

                    if (null==method) continue;
                    switch (method) {
                        case ORIGIN_COLUMN:
                        case EXPLODE_MAP:
                            if (sb.length()>0) sb.append(" AND ");
                            sb.append(name).append("=?");
                            break;
                        case CONSTANT_COLUMN:
                            if (null!=defaultValue) {
                                if (sb.length() > 0) sb.append(" AND ");
                                sb.append(name).append("=").append(defaultValue);
                            }
                            break;
                    }
                }
                return sb.toString();
        }
        return null;
    }

    public List<String> getPKNames(Side side) {
        switch (side) {
            case ORIGIN:
                return originPKNames;
            case TARGET:
                return targetPKNames;
            default:
                throw new RuntimeException("Unknown side: "+side);
        }
    }

    public List<Integer> getPKIndexesToBind(Side side) {
        switch (side) {
            case ORIGIN:
                return originPKIndexesToBind;
            case TARGET:
                return targetPKIndexesToBind;
            default:
                throw new RuntimeException("Unknown side: "+side);
        }
    }

    public List<MigrateDataType> getPKTypes(Side side) {
        switch (side) {
            case ORIGIN:
                return originPKTypes;
            case TARGET:
                return targetPKTypes;
            default:
                throw new RuntimeException("Unknown side: "+side);
        }
    }

    public List<Record> toValidRecordList(Record record) {
        if (null==record || !record.isValid())
            return new ArrayList<>(0);

        List<Record> recordSet;
        if (record.getPk().canExplode()) {
            recordSet = record.getPk().explode().stream()
                    .filter(pk -> !pk.isError())
                    .map(pk -> new Record(pk, record.getOriginRow(), record.getTargetRow()))
                    .collect(Collectors.toList());
        } else {
            recordSet = Arrays.asList(record);
        }
        return recordSet;
    }

    protected Long getDefaultForMissingTimestamp() {
        return defaultForMissingTimestamp;
    }

    protected String getDefaultForMissingString() {
        return defaultForMissingString;
    }

    public Integer getExplodeMapTargetPKIndex() {return explodeMapTargetPKIndex;}

    private List<Object> getTargetPKValuesFromOriginColumnLookupMethod(Row originRow, List<Object> defaultValues) {
        List<Object> newValues = new ArrayList<>(defaultValues);
        for (int i = 0; i< targetPKLookupMethods.size(); i++) {
            if (targetPKLookupMethods.get(i) != LookupMethod.ORIGIN_COLUMN)
                continue;

            Object value = cqlHelper.getData(targetPKTypes.get(i), targetToOriginPKIndexes.get(i),originRow);
            newValues.set(i, value);
        }
        return newValues;
    }

    private void setPKNamesAndTypes(PropertyHelper propertyHelper) {
        targetPKNames.addAll(propertyHelper.getStringList(KnownProperties.TARGET_PRIMARY_KEY));
        targetPKTypes.addAll(propertyHelper.getMigrationTypeList(KnownProperties.TARGET_PRIMARY_KEY_TYPES));
        if (targetPKNames.isEmpty() || targetPKTypes.size() != targetPKNames.size()) {
            throw new RuntimeException("Target primary key and/or types is not defined or not valid, see "+KnownProperties.TARGET_PRIMARY_KEY+" and "+KnownProperties.TARGET_PRIMARY_KEY_TYPES);
        }

        originPKNames.addAll(propertyHelper.getStringList(KnownProperties.ORIGIN_PRIMARY_KEY));
        originPKTypes.addAll(propertyHelper.getMigrationTypeList(KnownProperties.ORIGIN_PRIMARY_KEY_TYPES));
        if (originPKNames.isEmpty() || originPKNames.size() != originPKTypes.size()) {
            throw new RuntimeException("Origin primary key and/or types is not defined or not valid, see "+KnownProperties.ORIGIN_PRIMARY_KEY+" and "+KnownProperties.ORIGIN_PRIMARY_KEY_TYPES);
        }
    }

    private Map<Object,Object> getExplodeMap(Row originRow) {
        if (explodeMapTargetKeyColumnIndex < 0) {
            return null;
        }
        return (Map<Object,Object>) cqlHelper.getData(originColumnTypes.get(explodeMapOriginColumnIndex), explodeMapOriginColumnIndex,originRow);
    }

    // As target columns can be renamed, but we expect the positions of origin and target columns to be the same
    // we first look up the index on the target, then we look up the name of the column at this index on the origin
    private List<Integer> targetToOriginColumnIndexes(PropertyHelper propertyHelper) {
        List<String> originColumnNames = propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES);
        List<String> targetColumnNames = propertyHelper.getStringList(KnownProperties.TARGET_COLUMN_NAMES);
        if (null==originColumnNames || null==targetColumnNames || originColumnNames.size()==0 || targetColumnNames.size()==0)
            throw new RuntimeException("Origin and target column names are not the same size, see "+KnownProperties.ORIGIN_COLUMN_NAMES+" and "+KnownProperties.TARGET_COLUMN_NAMES);
        if (null==this.originColumnTypes || null==this.targetColumnTypes || this.originColumnTypes.size()==0 || this.targetColumnTypes.size()==0)
            throw new RuntimeException("Origin and target column types are not the same size, see "+KnownProperties.ORIGIN_COLUMN_TYPES+" and "+KnownProperties.TARGET_COLUMN_TYPES);

        List<Integer> targetToOriginColumnIndexes = new ArrayList<>(targetColumnNames.size());
        // Iterate over the target column names
        for (int i = 0; i< targetColumnNames.size(); i++) {
            /*
             * 1. If the target name is found on the origin, consider these the same; the indexes may not match.
             * 2. If the target name is not found on the origin, but the target type at element i matches the
             *    origin type at element i, consider these the same.
             * 3. Otherwise, they are not a match.
             */
            String targetColumnName = targetColumnNames.get(i);
            if (originColumnNames.contains(targetColumnName)) {
                targetToOriginColumnIndexes.add(originColumnNames.indexOf(targetColumnName));
            } else if (i < this.originColumnTypes.size() && this.targetColumnTypes.get(i).equals(this.originColumnTypes.get(i))) {
                targetToOriginColumnIndexes.add(i);
            } else {
                targetToOriginColumnIndexes.add(null);
            }
        }
        return targetToOriginColumnIndexes;
    }

    public List<Integer> getTargetToOriginColumnIndexes() {
        return targetToOriginColumnIndexes;
    }

    public List<MigrateDataType> getTargetColumnTypes() {
        return targetColumnTypes;
    }

    private List<Integer> targetToOriginPKIndexes(PropertyHelper propertyHelper) {
        List<String> targetColumnNames = propertyHelper.getStringList(KnownProperties.TARGET_COLUMN_NAMES);
        List<Integer> rtn = new ArrayList<>();
        for (int i = 0; i< targetPKNames.size(); i++) {
            if (targetPKLookupMethods.get(i) != LookupMethod.ORIGIN_COLUMN) {
                rtn.add(null);
            }
            else {
                int targetIndex = targetColumnNames.indexOf(targetPKNames.get(i));
                rtn.add(targetToOriginColumnIndexes.get(targetIndex));
            }
        }
        return rtn;
    }

    // This fills the PKLookupMethods lists with either ORIGIN_COLUMN or null.
    private void setOriginColumnLookupMethod(PropertyHelper propertyHelper) {
        List<String> originColumnNames = propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES);
        List<String> targetColumnNames = propertyHelper.getStringList(KnownProperties.TARGET_COLUMN_NAMES);
        if (null==originColumnNames || originColumnNames.isEmpty() || null==targetColumnNames || targetColumnNames.isEmpty())
            throw new RuntimeException("Origin and/or column names are not set, see "+KnownProperties.ORIGIN_COLUMN_NAMES+" and "+KnownProperties.TARGET_COLUMN_NAMES);

        if (null==targetToOriginColumnIndexes || targetToOriginColumnIndexes.isEmpty())
            throw new RuntimeException("Target to origin column indexes are not set, setTargetToOriginColumnIndexes must be called first");

        if (null==originPKLookupMethods || originPKLookupMethods.size() != originPKNames.size())
            throw new RuntimeException("originPKLookupMethods is not set, or does not have the same size as originPKNames");

        // Origin PK columns are expected to be found on originColumnNames; if not, it could be because
        // the origin PK defaulted from the target PK, and the column is added as part of a feature
        // (e.g. explode map). In that case, we will set the lookup to null.
        for (int i=0; i<originPKNames.size(); i++) {
            if (originColumnNames.contains(originPKNames.get(i)))
                this.originPKLookupMethods.set(i,LookupMethod.ORIGIN_COLUMN);
        }

        // Target PK columns may or may not be found on the originColumnNames. But all target columns should be
        // on targetToOriginColumnIndexes, and if the value on that list is not null it means that the column
        // has a corresponding column on the origin.
        for (int i=0; i<targetPKNames.size(); i++) {
            int targetColumnIndex = targetColumnNames.indexOf(targetPKNames.get(i));
            if (targetColumnIndex >=0 && targetToOriginColumnIndexes.get(targetColumnIndex) != null)
                this.targetPKLookupMethods.set(i,LookupMethod.ORIGIN_COLUMN);
        }
    }

    private void setConstantColumns() {
        Feature constantColumnFeature;
        if (cqlHelper.isFeatureEnabled(Featureset.CONSTANT_COLUMNS)) {
            constantColumnFeature = cqlHelper.getFeature(Featureset.CONSTANT_COLUMNS);
            List<String> constantColumnNames = constantColumnFeature.getStringList(ConstantColumns.Property.COLUMN_NAMES);
            List<String> constantColumnValues = constantColumnFeature.getStringList(ConstantColumns.Property.COLUMN_VALUES);

            for (int i = 0; i< targetPKNames.size(); i++) {
                String pkColumn = targetPKNames.get(i);
                if (constantColumnNames.contains(pkColumn)) {
                    this.targetDefaultValues.set(i, constantColumnValues.get(constantColumnNames.indexOf(pkColumn)));
                    this.targetPKLookupMethods.set(i, LookupMethod.CONSTANT_COLUMN);
                }
            }
        }
    }

    private Integer setExplodeMapMethods_getTargetKeyColumnIndex() {
        Feature explodeMapFeature;
        if (cqlHelper.isFeatureEnabled(Featureset.EXPLODE_MAP)) {
            explodeMapFeature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);

            String explodeMapKeyColumn = explodeMapFeature.getString(ExplodeMap.Property.KEY_COLUMN_NAME);
            for (int i = 0; i< targetPKNames.size(); i++) {
                String pkColumn = targetPKNames.get(i);
                if (pkColumn.equals(explodeMapKeyColumn)) {
                    this.targetPKLookupMethods.set(i, LookupMethod.EXPLODE_MAP);
                    return i;
                }
            }
        }
        return -1;
    }

    private Integer getExplodeMapOriginColumnIndex() {
        Feature explodeMapFeature;
        if (cqlHelper.isFeatureEnabled(Featureset.EXPLODE_MAP)) {
            explodeMapFeature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);
            return explodeMapFeature.getInteger(ExplodeMap.Property.MAP_COLUMN_INDEX);
        }
        return -1;
    }

    private List<Integer> getIndexesToBind(Side side) {
        List<Integer> indexesToBind = new ArrayList<>();
        List<LookupMethod> lookupMethods = (side == Side.ORIGIN) ? originPKLookupMethods : targetPKLookupMethods;
        for (int i = 0; i< lookupMethods.size(); i++) {
            LookupMethod method = lookupMethods.get(i);
            if (null != method && method != LookupMethod.CONSTANT_COLUMN)
                indexesToBind.add(i);
        }
        return indexesToBind;
    }

    private OriginSelectByPartitionRangeStatement getOriginSelectByPartitionRangeStatement() {
        if (null==originSelectByPartitionRangeStatement) {
            this.originSelectByPartitionRangeStatement = cqlHelper.getOriginSelectByPartitionRangeStatement();
        }
        return originSelectByPartitionRangeStatement;
    }

    private void scrubLookupMethods() {
        for (int i=0; i<targetPKLookupMethods.size(); i++) {
            if (null==targetPKLookupMethods.get(i)) {
                logger.warn("Target PK column "+targetPKNames.get(i)+" could not find a lookup type, and will be ignored.");
                targetPKLookupMethods.remove(i);
                targetPKNames.remove(i);
                targetDefaultValues.remove(i);
                targetPKTypes.remove(i);
            }
        }

        for (int i=0; i<originPKLookupMethods.size(); i++) {
            LookupMethod method = originPKLookupMethods.get(i);
            if (null==method || method == LookupMethod.CONSTANT_COLUMN) {
                if (null==method)
                    logger.warn("Origin PK column "+originPKNames.get(i)+" could not find a lookup type, and will be ignored.");
                originPKLookupMethods.remove(i);
                originPKNames.remove(i);
                originPKTypes.remove(i);
            }
        }
    }
}
