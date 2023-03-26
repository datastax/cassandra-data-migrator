package datastax.astra.migrate.cql.features;

import datastax.astra.migrate.properties.KnownProperties;
import datastax.astra.migrate.properties.PropertyHelper;

public class OriginFilterCondition extends AbstractFeature  {
        public enum Property {
            CONDITION
        }

        @Override
        public boolean initialize(PropertyHelper propertyHelper) {

            String filterCondition = propertyHelper.getString(KnownProperties.ORIGIN_FILTER_CONDITION);
            if (null!=filterCondition && !filterCondition.isEmpty()) {
                filterCondition = filterCondition.trim();
                if (!filterCondition.toUpperCase().startsWith("AND")) {
                    filterCondition = " AND " + filterCondition;
                }
            }
            else {
                filterCondition = "";
            }
            putString(Property.CONDITION, filterCondition);

            isInitialized=true;
            isEnabled=(null != filterCondition && !filterCondition.isEmpty());
            return true;
        }
}
