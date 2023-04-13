package datastax.cdm.feature;

import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.KnownProperties;
import datastax.cdm.properties.PropertyHelper;

public class OriginFilterCondition extends AbstractFeature  {
        public enum Property {
            CONDITION
        }

        @Override
        public boolean initialize(PropertyHelper propertyHelper, CqlHelper cqlHelper) {

            String filterCondition = propertyHelper.getString(KnownProperties.FILTER_CQL_WHERE_CONDITION);
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
