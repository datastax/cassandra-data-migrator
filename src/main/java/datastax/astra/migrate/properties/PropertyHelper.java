package datastax.astra.migrate.properties;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import datastax.astra.migrate.MigrateDataType;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class PropertyHelper extends KnownProperties{
    private static PropertyHelper instance = null;

//    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
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

    protected Object get(String propertyName, PropertyType expectedType) {
        if (null == propertyName
                || null == expectedType
                || expectedType != getType(propertyName)) {
            return null;
        }
        Object currentProperty;
        synchronized (PropertyHelper.class){
            currentProperty = propertyMap.get(propertyName);
        }

        if (null == currentProperty) {
            return null;
        }
        if (validateType(expectedType, currentProperty)) {
            return currentProperty;
        } else {
            return null;
        }
    }

    public String getString(String propertyName) {
        return (String) get(propertyName, PropertyType.STRING);
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
        for (Number n : getNumberList(propertyName)) {
            if (null == n)
                return null;
            i = toInteger(n);
            if (null == i)
                return null;
            intList.add(i);
        }
        return intList;
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
        if (null == propertyName)
            return null;
        Object propertyValue = get(propertyName, getType(propertyName));
        if (null == propertyValue)
            return null;
        switch (getType(propertyName)) {
            case STRING:
                return (String) propertyValue;
            case STRING_LIST:
            case NUMBER_LIST:
            case MIGRATION_TYPE_LIST:
                return StringUtils.join((List<?>) propertyValue, ",");
            case NUMBER:
            case BOOLEAN:
            case MIGRATION_TYPE:
            default:
                return propertyValue.toString();
        }
    }

    protected void loadSparkConf() {
        boolean fullyLoaded = true;
        for (Tuple2<String,String> kvp : sparkConf.getAll()) {
            String scKey = kvp._1();
            String scValue = kvp._2();
            Object setValue = null;
            boolean parsedAll = true;
            MigrateDataType mdt;

            if (isKnown(scKey)) {
                switch (getType(scKey)) {
                    case STRING:
                        setValue = setProperty(scKey, sparkConf.get(scKey, getDefault(scKey)));
                        break;
                    case STRING_LIST:
                        setValue = setProperty(scKey, sparkConf.get(scKey, getDefault(scKey)).split(","));
                        break;
                    case NUMBER:
                        try {
                            setValue = setProperty(scKey, sparkConf.getLong(scKey, Long.parseLong(getDefault(scKey))));
                        } catch (NumberFormatException e) {
//                            logger.warn("Unable to parse number for property: " + scKey + ", value: " + scValue + " with type: " + getType(scKey));
                        }
                        break;
                    case NUMBER_LIST:
                        List<Number> numList = new ArrayList<>();
                        for (String s : sparkConf.get(scKey, getDefault(scKey)).split(",")) {
                            try {
                                numList.add(Long.parseLong(s.trim()));
                            } catch (NumberFormatException e) {
//                                logger.warn("Unable to parse number: " + s + " for property: " + scKey + ", value: " + scValue + " with type: " + getType(scKey));
                                parsedAll = false;
                            }
                        }
                        if (parsedAll) {
                            setValue = setProperty(scKey, numList);
                        }
                        break;
                    case BOOLEAN:
                        setValue = setProperty(scKey, sparkConf.getBoolean(scKey, new Boolean(getDefault(scKey))));
                        break;
                    case MIGRATION_TYPE:
                        mdt = new MigrateDataType(sparkConf.get(scKey, getDefault(scKey)));
                        if (mdt.isValid()) {
                            setValue = setProperty(scKey, mdt);
                        }
                        break;
                    case MIGRATION_TYPE_LIST:
                        List<MigrateDataType> mdtList = new ArrayList<>();
                        for (String s : sparkConf.get(scKey, getDefault(scKey)).split(",")) {
                            mdt = new MigrateDataType(s);
                            if (mdt.isValid()) {
                                mdtList.add(mdt);
                            } else {
//                                logger.warn("Unable to parse MigrateDataType: " + s + " for property: " + scKey + ", value: " + scValue + " with type: " + getType(scKey));
                                parsedAll = false;
                            }
                        }
                        if (parsedAll) {
                            setValue = setProperty(scKey, mdtList);
                        }
                        break;
                    default:
                        break;
                }
                if (null == setValue) {
//                    logger.warn("Unable to set property: " + scKey + ", value: " + scValue + " with type: " + getType(scKey));
                    fullyLoaded = false;
                }
            }
            else {
//                logger.warn("Unknown property: " + scKey + ", value: " + scValue);
                fullyLoaded = false;
            }
        }
        this.sparkConfFullyLoaded = fullyLoaded;
    }

    protected boolean validateType(PropertyType expectedType, Object currentProperty) {
        switch (expectedType) {
            case STRING:
                if (currentProperty instanceof String) {
                    return true;
                }
                break;
            case STRING_LIST:
                if (currentProperty instanceof List<?>) {
                    List<?> list = (List<?>) currentProperty;
                    if (list.isEmpty()) {
                        return false;
                    } else {
                        for (Object o : list) {
                            if (!(o instanceof String)) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                break;
            case NUMBER:
                if (currentProperty instanceof Number) {
                    return true;
                }
                break;
            case NUMBER_LIST:
                if (currentProperty instanceof List<?>) {
                    List<?> list = (List<?>) currentProperty;
                    if (list.isEmpty()) {
                        return false;
                    } else {
                        for (Object o : list) {
                            if (!(o instanceof Number)) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                break;
            case BOOLEAN:
                if (currentProperty instanceof Boolean) {
                    return true;
                }
                break;
            case MIGRATION_TYPE:
                if ((currentProperty instanceof MigrateDataType) && ((MigrateDataType) currentProperty).isValid()) {
                    return true;
                }
                break;
            case MIGRATION_TYPE_LIST:
                if (currentProperty instanceof List<?>) {
                    List<?> list = (List<?>) currentProperty;
                    if (list.isEmpty()) {
                        return false;
                    } else {
                        for (Object o : list) {
                            if (!(o instanceof MigrateDataType) || !((MigrateDataType) o).isValid()) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                break;
            default:
                break;
        }
        return false;
    }

    private Integer toInteger(Number n) {
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

    protected Map<String,Object> getPropertyMap() {
        return propertyMap;
    }

    public boolean isSparkConfFullyLoaded() {
        return sparkConfFullyLoaded;
    }
}
