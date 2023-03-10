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
        PropertyType expectedType = getType(propertyName);
        if (null == expectedType || null == propertyValue) {
            return null;
        }
        Boolean typesMatch = false;
        switch (expectedType) {
            case STRING:
                typesMatch = propertyValue instanceof String;
                break;
            case STRING_LIST:
                if (propertyValue instanceof List<?>) {
                    List<?> list = (List<?>) propertyValue;
                    if (list.isEmpty()) {
                        typesMatch = false;
                    } else {
                        typesMatch = list.get(0) instanceof String;
                    }
                }
                break;
            case NUMBER:
                typesMatch = propertyValue instanceof Number;
            case NUMBER_LIST:
                if (propertyValue instanceof List<?>) {
                    List<?> list = (List<?>) propertyValue;
                    if (list.isEmpty()) {
                        typesMatch = false;
                    } else {
                        typesMatch = list.get(0) instanceof Number;
                    }
                }
                break;
            case BOOLEAN:
                typesMatch = propertyValue instanceof Boolean;
                break;
            case MIGRATION_TYPE:
                typesMatch = propertyValue instanceof MigrateDataType;
                break;
            case MIGRATION_TYPE_LIST:
                if (propertyValue instanceof List<?>) {
                    List<?> list = (List<?>) propertyValue;
                    if (list.isEmpty()) {
                        typesMatch = false;
                    } else {
                        typesMatch = list.get(0) instanceof MigrateDataType;
                    }
                }
                break;
            default:
                break;
        }
        if (typesMatch) {
            propertyMap.put(propertyName, propertyValue);
            return propertyValue;
        } else {
            return null;
        }
    }
//
//    public Integer getInteger(String propertyName, Boolean setDefaultIfMissing) {
//        Object currentProperty = properties.get(propertyName);
//        if (null == currentProperty) {
//            if (setDefaultIfMissing) {
//                Integer defaultValue = (Integer) super.getDefaultValue(propertyName);
//                properties.put(propertyName, defaultValue);
//                return defaultValue;
//            } else {
//                return null;
//            }
//        }
//
//        return (Integer) properties.get(propertyName);
//    }

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
}
