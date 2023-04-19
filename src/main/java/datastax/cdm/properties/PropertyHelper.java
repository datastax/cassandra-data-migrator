package datastax.cdm.properties;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.job.MigrateDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.*;

public final class PropertyHelper extends KnownProperties{
    private static PropertyHelper instance = null;

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Map<String,Object> propertyMap;
    private volatile SparkConf sparkConf;
    private boolean sparkConfFullyLoaded = false;

    // As this is a singleton class, the constructor is private
    private PropertyHelper() {
        super();
        propertyMap = new HashMap<>();
    }

    public static PropertyHelper getInstance() {
        if (instance == null) {
            synchronized (PropertyHelper.class) {
                if (instance == null) {
                    instance = new PropertyHelper();
                }
            }
        }
        return instance;
    }

    public static PropertyHelper getInstance(SparkConf sc) {
        instance = getInstance();
        instance.initializeSparkConf(sc);
        return instance;
    }

    public static void destroyInstance() {
        if (instance != null) {
            synchronized (PropertyHelper.class) {
                if (instance != null) {
                    instance = null;
                }
            }
        }
    }

    /**
     * Loads the SparkConf into the propertyMap, but only
     * if the SparkConf has not already been loaded.
     *
     * @param sc
     */
    public void initializeSparkConf(SparkConf sc) {
        if (null == sc)
            throw new IllegalArgumentException("SparkConf cannot be null");

        if (null == this.sparkConf)
            synchronized (PropertyHelper.class) {
                if (null == this.sparkConf) {
                    this.sparkConf = sc;
                    loadSparkConf();
                }
            }
    }

    /**
     * Sets a property value if it is of the correct and known type.
     * For _LIST types, the property will only be set if the list is not empty.
     * @param propertyName
     * @param propertyValue
     * @return propertyValue if it is of the correct type, null otherwise
     */
    public Object setProperty(String propertyName, Object propertyValue) {
        if (null == propertyName ||
                null == propertyValue)
            return null;
        PropertyType expectedType = getType(propertyName);
        if (null == expectedType) {
            return null;
        }

        boolean typesMatch = validateType(expectedType, propertyValue);
        if (!typesMatch)
            return null;

        synchronized (PropertyHelper.class) {
            propertyMap.put(propertyName, propertyValue);
        }
        return propertyValue;
    }

    protected Object get(String propertyName) {
        if (null == propertyName)
            return null;

        Object currentProperty;
        synchronized (PropertyHelper.class){
            currentProperty = propertyMap.get(propertyName);
        }
        return currentProperty;
    }

    protected Object get(String propertyName, PropertyType expectedType) {
        if (null == propertyName
                || null == expectedType
                || expectedType != getType(propertyName)) {
            return null;
        }
        Object currentProperty = get(propertyName);

        if (validateType(expectedType, currentProperty)) {
            return currentProperty;
        } else {
            return null;
        }
    }

    public String getString(String propertyName) {
        String rtn = (String) get(propertyName, PropertyType.STRING);
        return (null == rtn) ? "" : rtn;
    }

    public List<String> getStringList(String propertyName) {
        return (List<String>) get(propertyName, PropertyType.STRING_LIST);
    }

    public Number getNumber(String propertyName) {
        return (Number) get(propertyName, PropertyType.NUMBER);
    }

    public Integer getInteger(String propertyName) {
        if (null==getNumber(propertyName)
                || PropertyType.NUMBER != getType(propertyName))
            return null;
        return toInteger(getNumber(propertyName));
    }

    public Long getLong(String propertyName) {
        if (null==getNumber(propertyName)
                || PropertyType.NUMBER != getType(propertyName))
            return null;
        return getNumber(propertyName).longValue();
    }

    public List<Number> getNumberList(String propertyName) {
        return (List<Number>) get(propertyName, PropertyType.NUMBER_LIST);
    }

    public List<Integer> getIntegerList(String propertyName) {
        List<Integer> intList = new ArrayList<>();
        Integer i;
        if (null==propertyName
                || PropertyType.NUMBER_LIST != getType(propertyName)
                || null==getNumberList(propertyName))
            return null;
        return toIntegerList(getNumberList(propertyName));
    }

    public Boolean getBoolean(String propertyName) {
        return (Boolean) get(propertyName, PropertyType.BOOLEAN);
    }

    public MigrateDataType getMigrationType(String propertyName) {
        return (MigrateDataType) get(propertyName, PropertyType.MIGRATION_TYPE);
    }

    public List<MigrateDataType> getMigrationTypeList(String propertyName) {
        return (List<MigrateDataType>) get(propertyName, PropertyType.MIGRATION_TYPE_LIST);
    }

    public String getAsString(String propertyName) {
        String rtn;
        if (null == propertyName)
            return null;
        PropertyType t = getType(propertyName);
        return asString(get(propertyName, t), t);
    }

    public static String asString(Object o, PropertyType t) {
        if (null==o || null==t) return "";
        String rtn = "";
        switch (t) {
            case STRING:
                rtn = (String) o;
                break;
            case STRING_LIST:
            case NUMBER_LIST:
            case MIGRATION_TYPE_LIST:
                rtn = StringUtils.join((List<?>) o, ",");
                break;
            case NUMBER:
            case BOOLEAN:
            case MIGRATION_TYPE:
            default:
                rtn = o.toString();
        }
        return (null == rtn) ? "" : rtn;
    }

    protected void loadSparkConf() {
        boolean fullyLoaded = true;
        Object setValue;

        logger.info("Processing explicitly set and known sparkConf properties");
        for (Tuple2<String,String> kvp : sparkConf.getAll()) {
            String scKey = kvp._1();
            String scValue = kvp._2();
;
            if (KnownProperties.isKnown(scKey)) {
                PropertyType propertyType = KnownProperties.getType(scKey);
                setValue = setProperty(scKey, KnownProperties.asType(propertyType,scValue));
                if (null == setValue) {
                    logger.error("Unable to set property: [" + scKey + "], value: [" + scValue + "] with type: [" + propertyType +"]");
                    fullyLoaded = false;
                } else {
                    logger.info("Known property [" + scKey + "] is configured with value [" + scValue + "] and is type [" + propertyType + "]");
                }
            }
        }

        logger.info("Adding any missing known properties that have default values");
        for (String knownProperty : KnownProperties.getTypeMap().keySet()) {
            if (null == get(knownProperty)) {
                Object defaultValue = KnownProperties.getDefault(knownProperty);
                if (null != defaultValue) {
                    logger.info("Setting known property [" + knownProperty + "] with default value [" + KnownProperties.getDefaultAsString(knownProperty) + "]");
                    setProperty(knownProperty, defaultValue);
                }
            }
        }

        if (fullyLoaded) {
            fullyLoaded = isValidConfig();
        }

        this.sparkConfFullyLoaded = fullyLoaded;
    }

    protected boolean isValidConfig() {
        boolean valid = true;

        // First check over the simple-to-discover required properties
        for (String requiredProperty : KnownProperties.getRequired()) {
            if (null == get(requiredProperty) || getAsString(requiredProperty).isEmpty()) {
                logger.error("Missing required property: " + requiredProperty);
                valid = false;
            }
        }

        // Check we have a configured origin connection
        if ( (null == get(KnownProperties.ORIGIN_CONNECT_HOST) && null == get(KnownProperties.ORIGIN_CONNECT_SCB)) ||
                getAsString(KnownProperties.ORIGIN_CONNECT_HOST).isEmpty() && getAsString(KnownProperties.ORIGIN_CONNECT_SCB).isEmpty()) {
            logger.error("Missing required property: " + KnownProperties.ORIGIN_CONNECT_HOST + " or " + KnownProperties.ORIGIN_CONNECT_SCB);
            valid = false;
        } else {
            // Validate TLS configuration is set if so-enabled
            if (null == get(KnownProperties.ORIGIN_CONNECT_SCB) && null != get(KnownProperties.ORIGIN_TLS_ENABLED) && getBoolean(KnownProperties.ORIGIN_TLS_ENABLED)) {
                for (String expectedProperty : new String[]{KnownProperties.ORIGIN_TLS_TRUSTSTORE_PATH, KnownProperties.ORIGIN_TLS_TRUSTSTORE_PASSWORD,
                        KnownProperties.ORIGIN_TLS_TRUSTSTORE_TYPE, KnownProperties.ORIGIN_TLS_KEYSTORE_PATH, KnownProperties.ORIGIN_TLS_KEYSTORE_PASSWORD,
                        KnownProperties.ORIGIN_TLS_ALGORITHMS}) {
                    if (null == get(expectedProperty) || getAsString(expectedProperty).isEmpty()) {
                        logger.error("TLS is enabled, but required value is not set: " + expectedProperty);
                        valid = false;
                    }
                }
            }
        }

        // Check we have a configured target connection
        if ( (null == get(KnownProperties.TARGET_CONNECT_HOST) && null == get(KnownProperties.TARGET_CONNECT_SCB)) ||
                getAsString(KnownProperties.TARGET_CONNECT_HOST).isEmpty() && getAsString(KnownProperties.TARGET_CONNECT_SCB).isEmpty()) {
            logger.error("Missing required property: " + KnownProperties.TARGET_CONNECT_HOST + " or " + KnownProperties.TARGET_CONNECT_SCB);
            valid = false;
        } else {
            // Validate TLS configuration is set if so-enabled
            if (null == get(KnownProperties.TARGET_CONNECT_SCB) && null != get(KnownProperties.TARGET_TLS_ENABLED) && getBoolean(KnownProperties.TARGET_TLS_ENABLED)) {
                for (String expectedProperty : new String[]{KnownProperties.TARGET_TLS_TRUSTSTORE_PATH, KnownProperties.TARGET_TLS_TRUSTSTORE_PASSWORD,
                        KnownProperties.TARGET_TLS_TRUSTSTORE_TYPE, KnownProperties.TARGET_TLS_KEYSTORE_PATH, KnownProperties.TARGET_TLS_KEYSTORE_PASSWORD,
                        KnownProperties.TARGET_TLS_ALGORITHMS}) {
                    if (null == get(expectedProperty) || getAsString(expectedProperty).isEmpty()) {
                        logger.error("TLS is enabled, but required value is not set: " + expectedProperty);
                        valid = false;
                    }
                }
            }
        }
        
        // Expecting these to normally be set, but it could be a valid configuration
        for (String expectedProperty : new String[]{KnownProperties.ORIGIN_CONNECT_USERNAME, KnownProperties.ORIGIN_CONNECT_PASSWORD, KnownProperties.TARGET_CONNECT_USERNAME, KnownProperties.TARGET_CONNECT_PASSWORD}) {
            if (null == get(expectedProperty) || getAsString(expectedProperty).isEmpty()) {
                logger.warn("Unusual this is not set: " + expectedProperty);
            }
        }

        return valid;
    }

    public static Integer toInteger(Number n) {
        if (n instanceof Integer
                || n instanceof Short
                || n instanceof Byte)
            return n.intValue();
        else if (n instanceof Long) {
            if ((Long) n >= Integer.MIN_VALUE && (Long) n <= Integer.MAX_VALUE) {
                return n.intValue();
            }
        }
        return null;
    }

    public static List<Integer> toIntegerList(List<Number> numberList) {
        List<Integer> intList = new ArrayList<>();
        if (null==numberList) return intList;
        Integer i;
        for (Number n : numberList) {
            i = toInteger(n);
            if (null == i)
                return null;
            intList.add(i);
        }
        return intList;
    }

    protected Map<String,Object> getPropertyMap() {
        return propertyMap;
    }

    public boolean isSparkConfFullyLoaded() {
        return sparkConfFullyLoaded;
    }

    public boolean meetsMinimum(String valueName, Integer testValue, Integer minimumValue) {
        if (null != minimumValue && null != testValue && testValue >= minimumValue)
            return true;
        logger.warn(valueName + " must be greater than or equal to " + minimumValue + ".  Current value does not meet this requirement: " + testValue);
        return false;
    }

}
