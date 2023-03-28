package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.MigrateDataType;
import datastax.astra.migrate.properties.PropertyHelper;
import java.util.List;

public interface Feature {

    /**
     * Initializes the feature based on properties
     * @param propertyHelper propertyHelper containing initialized properties
     * @return true if the feature is initialized and valid, false otherwise
     */
    public boolean initialize(PropertyHelper propertyHelper);

    /**
     * Indicates if feature is enabled.
     * @return true if the feature is enabled, false otherwise
     * @throws RuntimeException if the feature is not initialized
     */
    public boolean isEnabled();

    /**
     * Convenience method to get a property as a String no matter the underlying type.
     * @param prop property to get as a String
     * @return the property value as a String, but an empty string if the configured String is null, or if the feature is not enabled
     * @throws RuntimeException if the feature is not initialized
     */
    public String getAsString(Enum<?> prop);

    /**
     * String property getter.
     * @param prop property to get, it should have been set as a String
     * @return the property value as a String, but null if
     *   1. the configured String is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a String
     */
    public String getString(Enum<?> prop);

    /**
     * StringList property getter.
     * @param prop property to get, it should have been set as a StringList
     * @return the property value as a StringList, but null if
     *   1. the configured StringList is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a StringList
     */
    public List<String> getStringList(Enum<?> prop);

    /**
     * MigrateDataTypeList property getter.
     * @param prop property to get, it should have been set as a MigrateDataTypeList
     * @return the property value as a MigrateDataTypeList, but null if
     *   1. the configured MigrateDataTypeList is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a MigrateDataTypeList
     */
    public List<MigrateDataType> getMigrateDataTypeList(Enum<?> prop);

    /**
     * NumberList property getter.
     * @param prop property to get, it should have been set as a NumberList
     * @return the property value as a NumberList, but null if
     *   1. the configured NumberList is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a NumberList
     */
    public List<Number> getNumberList(Enum<?> prop);

    /**
     * IntegerList property getter.
     * @param prop property to get, it should have been set as a NumberList
     * @return the property value as a NumberList, but null if
     *   1. the configured NumberList is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a NumberList
     *   4. if the any value within the NumberList cannot be represented as an Integer
     */
    public List<Integer> getIntegerList(Enum<?> prop);

    /**
     * Boolean property getter.
     * @param prop property to get, it should have been set as a Boolean
     * @return the property value as a Boolean, but null if
     *   1. the configured Boolean is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a Boolean
     */
    public Boolean getBoolean(Enum<?> prop);

    /**
     * Integer property getter.
     * @param prop property to get, it should have been set as a Number
     * @return the property value as a Number, but null if
     *   1. the configured Number is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a Number
     *   4. if the value cannot be represented as an Integer
     */
    public Integer getInteger(Enum<?> prop);

    /**
     * Number property getter.
     * @param prop property to get, it should have been set as a Number
     * @return the property value as a Number, but null if
     *  1. the configured Number is null, or
     *  2. if the feature is not enabled, or
     *  3. if the property is not a Number
     */
    public Number getNumber(Enum<?> prop);

    /**
     * MigrateDataType property getter.
     * @param prop property to get, it should have been set as a MigrateDataType
     * @return the property value as a MigrateDataType, but null if
     *   1. the configured MigrateDataType is null, or
     *   2. if the feature is not enabled, or
     *   3. if the property is not a MigrateDataType
     */
    public MigrateDataType getMigrateDataType(Enum<?> prop);

    /**
     * Generic feature function that can be used to implement any feature specific functionality.
     * @param function Feature-specific name of the feature to call
     * @param args Feature- and function-specific arguments
     * @return Function-specific
     */
    public Object featureFunction(Enum<?> function, Object... args);
}
