package com.datastax.cdm.feature;

import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.cdm.cql.CqlHelper;

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
