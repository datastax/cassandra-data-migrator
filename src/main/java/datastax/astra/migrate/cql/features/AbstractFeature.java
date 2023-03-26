package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public abstract class AbstractFeature implements Feature {

    protected boolean isEnabled = false;
    protected boolean isInitialized = false;

    private final Map<Enum<?>,String> strings;
    private final Map<Enum<?>,Boolean> booleans;
    private final Map<Enum<?>,Number> numbers;
    private final Map<Enum<?>,MigrateDataType> migrateDataTypes;
    private final Map<Enum<?>,List<String>> stringLists;
    private final Map<Enum<?>,List<Number>> numberLists;
    private final Map<Enum<?>,List<MigrateDataType>> migrateDataTypeLists;
    private final Map<Enum<?>, KnownProperties.PropertyType> propertyTypes;

    public AbstractFeature() {
        this.strings = new HashMap<>();
        this.booleans = new HashMap<>();
        this.numbers = new HashMap<>();
        this.migrateDataTypes = new HashMap<>();
        this.stringLists = new HashMap<>();
        this.numberLists = new HashMap<>();
        this.migrateDataTypeLists = new HashMap<>();
        this.propertyTypes = new HashMap<>();
    }

    @Override
    public boolean isEnabled() {
        if (!isInitialized) throw new RuntimeException("Feature not initialized");
        return isEnabled;
    }

    @Override
    public String getAsString(Enum<?> key) {
        if (!isEnabled) return "";
        KnownProperties.PropertyType type = propertyTypes.get(key);
        if (null==type) return "";
        String rtn = "";
        switch(type) {
            case STRING: rtn=PropertyHelper.asString(getString(key), KnownProperties.PropertyType.STRING); break;
            case BOOLEAN: rtn=PropertyHelper.asString(getBoolean(key), KnownProperties.PropertyType.BOOLEAN); break;
            case NUMBER: rtn=PropertyHelper.asString(getNumber(key), KnownProperties.PropertyType.NUMBER); break;
            case MIGRATION_TYPE: rtn=PropertyHelper.asString(getMigrateDataType(key), KnownProperties.PropertyType.MIGRATION_TYPE); break;
            case STRING_LIST: rtn=PropertyHelper.asString(getStringList(key), KnownProperties.PropertyType.STRING_LIST); break;
            case NUMBER_LIST: rtn=PropertyHelper.asString(getNumberList(key), KnownProperties.PropertyType.NUMBER_LIST); break;
            case MIGRATION_TYPE_LIST: rtn=PropertyHelper.asString(getMigrateDataTypeList(key), KnownProperties.PropertyType.MIGRATION_TYPE_LIST); break;
        }
        return rtn;
    }

    @Override
    public String getString(Enum<?> key) {
        return !isEnabled ? null : strings.get(key);
    }

    @Override
    public Boolean getBoolean(Enum<?> key) {
        return !isEnabled ? null : booleans.get(key);
    }

    @Override
    public Number getNumber(Enum<?> key) {
        return !isEnabled ? null : numbers.get(key);
    }

    @Override
    public Integer getInteger(Enum<?> key) {
        return !isEnabled ? null : PropertyHelper.toInteger(numbers.get(key));
    }

    @Override
    public MigrateDataType getMigrateDataType(Enum<?> key) {
        return !isEnabled ? null : migrateDataTypes.get(key);
    }

    @Override
    public List<String> getStringList(Enum<?> key) {
        return !isEnabled ? null : stringLists.get(key);
    }

    @Override
    public List<Number> getNumberList(Enum<?> key) {
        return !isEnabled ? null : numberLists.get(key);
    }

    @Override
    public List<Integer> getIntegerList(Enum<?> key) {
        return !isEnabled ? null : PropertyHelper.toIntegerList(numberLists.get(key));
    }

    @Override
    public List<MigrateDataType> getMigrateDataTypeList(Enum key) {
        return !isEnabled ? null : migrateDataTypeLists.get(key);
    }

    protected void putString(Enum<?> key, String value) {
        strings.put(key, value);
        propertyTypes.put(key, KnownProperties.PropertyType.STRING);
    }

    protected void putBoolean(Enum<?> key, Boolean value) {
        booleans.put(key, value);
        propertyTypes.put(key, KnownProperties.PropertyType.BOOLEAN);
    }

    protected void putNumber(Enum<?> key, Number value) {
        numbers.put(key, value);
        propertyTypes.put(key, KnownProperties.PropertyType.NUMBER);
    }

    protected void putMigrateDataType(Enum<?> key, MigrateDataType value) {
        migrateDataTypes.put(key, value);
        propertyTypes.put(key, KnownProperties.PropertyType.MIGRATION_TYPE);
    }

    protected void putStringList(Enum<?> key, List<String> value) {
        stringLists.put(key, value);
        propertyTypes.put(key, KnownProperties.PropertyType.STRING_LIST);
    }

    protected void putNumberList(Enum<?> key, List<Number> value) {
        numberLists.put(key, value);
        propertyTypes.put(key, KnownProperties.PropertyType.NUMBER_LIST);
    }

    protected void putMigrateDataTypeList(Enum<?> key, List<MigrateDataType> value) {
        migrateDataTypeLists.put(key, value);
        propertyTypes.put(key, KnownProperties.PropertyType.MIGRATION_TYPE_LIST);
    }

}
