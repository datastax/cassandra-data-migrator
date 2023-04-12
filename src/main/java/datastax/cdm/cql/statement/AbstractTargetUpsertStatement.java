package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.cql.*;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.data.EnhancedPK;
import datastax.cdm.data.Record;
import datastax.cdm.feature.*;
import datastax.cdm.properties.ColumnsKeysTypes;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public abstract class AbstractTargetUpsertStatement extends BaseCdmStatement {
    protected final List<String> targetColumnNames = new ArrayList<>();
    protected final List<String> originColumnNames = new ArrayList<>();
    protected final List<String> constantColumnNames = new ArrayList<>();
    protected final List<String> constantColumnValues = new ArrayList<>();

    protected final List<MigrateDataType> targetColumnTypes = new ArrayList<>();
    protected final List<MigrateDataType> originColumnTypes = new ArrayList<>();

    protected final List<Integer> counterIndexes = new ArrayList<>();

    protected boolean usingCounter = false;
    protected boolean usingTTL = false;
    protected boolean usingWriteTime = false;
    protected Feature constantColumnFeature;
    protected Feature explodeMapFeature;

    protected int bindIndex = 0;
    protected int explodeMapKeyIndex = -1;
    protected int explodeMapValueIndex = -1;

    protected abstract String buildStatement();
    protected abstract BoundStatement bind(Row originRow, Row targetRow, Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue);

    public AbstractTargetUpsertStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        super(propertyHelper, cqlHelper);

        this.session = cqlHelper.getTargetSession();

        constantColumnFeature = cqlHelper.getFeature(Featureset.CONSTANT_COLUMNS);
        explodeMapFeature = cqlHelper.getFeature(Featureset.EXPLODE_MAP);

        setTTLAndWriteTimeNames();
        setNamesAndTypes();
        setConstantColumns();
        setExplodeMapIndexes();
        setCounterIndexes();

        this.statement = buildStatement();
    }

    public BoundStatement bindRecord(Record record) {
        if (null==record)
            throw new RuntimeException("record is null");

        EnhancedPK pk = record.getPk();
        Row originRow = record.getOriginRow();
        Row targetRow = record.getTargetRow();

        return bind(originRow, targetRow, pk.getTTL(), pk.getWriteTimestamp(), pk.getExplodeMapKey(), pk.getExplodeMapValue());
    }

    public CompletionStage<AsyncResultSet> executeAsync(Statement<?> statement) {
        return session.executeAsync(statement);
    }

    public ResultSet putRecord(Record record) {
        BoundStatement boundStatement = bindRecord(record);
        return session.execute(boundStatement);
    }

    protected String usingTTLTimestamp() {
        StringBuilder sb;
        if (usingTTL || usingWriteTime)
            sb = new StringBuilder(" USING ");
        else
            return "";

        if (usingTTL)
            sb.append("TTL ?");

        if (usingTTL && usingWriteTime)
            sb.append(" AND ");

        if (usingWriteTime)
            sb.append("TIMESTAMP ?");

        return sb.toString();
    }

    private void setNamesAndTypes() {
        originColumnNames.addAll(ColumnsKeysTypes.getOriginColumnNames(propertyHelper));
        originColumnTypes.addAll(ColumnsKeysTypes.getOriginColumnTypes(propertyHelper));
        targetColumnNames.addAll(ColumnsKeysTypes.getTargetColumnNames(propertyHelper));
        targetColumnTypes.addAll(ColumnsKeysTypes.getTargetColumnTypes(propertyHelper));
    }

    private void setConstantColumns() {
        if (FeatureFactory.isEnabled(constantColumnFeature)) {
            constantColumnNames.addAll(constantColumnFeature.getStringList(ConstantColumns.Property.COLUMN_NAMES));
            constantColumnValues.addAll(constantColumnFeature.getStringList(ConstantColumns.Property.COLUMN_VALUES));
        }

        if (constantColumnNames.size() != constantColumnValues.size()) {
            throw new RuntimeException("Constant column names and values are not the same size.");
        }
    }

    private void setTTLAndWriteTimeNames() {
        List<Integer> ttlColumnNames = propertyHelper.getIntegerList(KnownProperties.ORIGIN_TTL_INDEXES);
        usingTTL = null!= ttlColumnNames && !ttlColumnNames.isEmpty();
        List<Integer> writeTimeColumnNames = propertyHelper.getIntegerList(KnownProperties.ORIGIN_WRITETIME_INDEXES);
        usingWriteTime = null!= writeTimeColumnNames && !writeTimeColumnNames.isEmpty();
    }

    private void setCounterIndexes() {
        List<Integer> originCounterIndexes = propertyHelper.getIntegerList(KnownProperties.ORIGIN_COUNTER_INDEXES);
        if (null==originCounterIndexes || originCounterIndexes.isEmpty())
            return;

        usingCounter = true;
        List<String> configuredColumnNames = propertyHelper.getStringList(KnownProperties.ORIGIN_COLUMN_NAMES);

        for (Integer index : originCounterIndexes) {
            if (index < 0 || index >= configuredColumnNames.size())
                throw new RuntimeException("Counter index "+index+" is out of range for origin columns "+originColumnNames+ " configured at "+KnownProperties.ORIGIN_COLUMN_NAMES);

            int newIndex = originColumnNames.indexOf(configuredColumnNames.get(index));
            if (newIndex < 0)
                // originColumnNames was a copy of ORIGIN_COLUMN_NAMES and then aligned with the target. We'd therefore
                // expect this index to be >=0, and if it isn't some head-scratching is required.
                throw new RuntimeException("This is a bug, please report it. Counter index "+index+" is not found in origin columns "+originColumnNames);

            this.counterIndexes.add(newIndex);
        }
    }

    protected void checkBindInputs(Integer ttl, Long writeTime, Object explodeMapKey, Object explodeMapValue) {
        if (usingTTL && null==ttl)
            throw new RuntimeException(KnownProperties.ORIGIN_TTL_INDEXES +" specified, but no TTL value was provided");

        if (usingWriteTime && null==writeTime)
            throw new RuntimeException(KnownProperties.ORIGIN_WRITETIME_INDEXES + " specified, but no WriteTime value was provided");

        if (FeatureFactory.isEnabled(explodeMapFeature)) {
            if (null==explodeMapKey)
                throw new RuntimeException("ExplodeMap is enabled, but no map key was provided");
            else if (!(explodeMapKey.getClass().equals(explodeMapFeature.getMigrateDataType(ExplodeMap.Property.KEY_COLUMN_TYPE).getTypeClass())))
                throw new RuntimeException("ExplodeMap is enabled, but the map key type provided "+explodeMapKey.getClass().getName()+" is not of the expected type "+explodeMapFeature.getMigrateDataType(ExplodeMap.Property.KEY_COLUMN_TYPE).getTypeClass().getName());

            if (null==explodeMapValue)
                throw new RuntimeException("ExplodeMap is enabled, but no map value was provided");
            else if (!(explodeMapValue.getClass().equals(explodeMapFeature.getMigrateDataType(ExplodeMap.Property.VALUE_COLUMN_TYPE).getTypeClass())))
                throw new RuntimeException("ExplodeMap is enabled, but the map value type provided "+explodeMapValue.getClass().getName()+" is not of the expected type "+explodeMapFeature.getMigrateDataType(ExplodeMap.Property.VALUE_COLUMN_TYPE).getTypeClass().getName());
        }
    }

    private void setExplodeMapIndexes() {
        int currentColumn = 0;
        for (String key : targetColumnNames) {
            if (FeatureFactory.isEnabled(explodeMapFeature)) {
                if (key.equals(explodeMapFeature.getString(ExplodeMap.Property.KEY_COLUMN_NAME)))
                    explodeMapKeyIndex = currentColumn;
                else if (key.equals(explodeMapFeature.getString(ExplodeMap.Property.VALUE_COLUMN_NAME)))
                    explodeMapValueIndex = currentColumn;
            }
            currentColumn++;
        }
    }

}
