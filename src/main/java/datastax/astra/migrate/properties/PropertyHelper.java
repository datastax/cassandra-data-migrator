package datastax.astra.migrate.properties;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import datastax.astra.migrate.MigrateDataType;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropertyHelper extends KnownProperties{
//    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Map<String,Object> propertyMap;
    private SparkConf sparkConf;
    private boolean sparkConfFullyLoaded = false;

    public PropertyHelper() {
        super();
        propertyMap = new HashMap<>();
    }

    public PropertyHelper(SparkConf sc) {
        this();
        setSparkConf(sc);
    }

    public boolean setSparkConf(SparkConf sc) {
        this.sparkConf = sc;
        return loadSparkConf();
    }

    public boolean isSparkConfFullyLoaded() {
        return sparkConfFullyLoaded;
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
        if (typesMatch) {
            propertyMap.put(propertyName, propertyValue);
            return propertyValue;
        } else {
            return null;
        }
    }

    protected Object get(String propertyName, PropertyType expectedType) {
        if (null == propertyName
                || null == expectedType
                || expectedType != getType(propertyName)) {
            return null;
        }
        Object currentProperty = propertyMap.get(propertyName);
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

    public List<Number> getNumberList(String propertyName) {
        return (List<Number>) get(propertyName, PropertyType.NUMBER_LIST);
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

    protected boolean loadSparkConf() {
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
                                numList.add(Long.parseLong(s));
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
                            mdt = new MigrateDataType(sparkConf.get(scKey, getDefault(scKey)));
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
        return fullyLoaded;
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
                        if (list.get(0) instanceof String) {
                            return true;
                        }
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
                        if (list.get(0) instanceof Number) {
                            return true;
                        }
                    }
                }
                break;
            case BOOLEAN:
                if (currentProperty instanceof Boolean) {
                    return true;
                }
                break;
            case MIGRATION_TYPE:
                if (currentProperty instanceof MigrateDataType) {
                    return true;
                }
                break;
            case MIGRATION_TYPE_LIST:
                if (currentProperty instanceof List<?>) {
                    List<?> list = (List<?>) currentProperty;
                    if (list.isEmpty()) {
                        return false;
                    } else {
                        if (list.get(0) instanceof MigrateDataType) {
                            return true;
                        }
                    }
                }
                break;
            default:
                break;
        }
        return false;
    }

    protected Map<String,Object> getPropertyMap() {
        return propertyMap;
    }
}
