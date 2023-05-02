package com.datastax.cdm.cql.statement;

import com.datastax.cdm.feature.UDTMapper;
import com.datastax.cdm.properties.PropertyHelper;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.cdm.feature.FeatureFactory;
import com.datastax.cdm.feature.Featureset;
import com.datastax.cdm.job.MigrateDataType;
import com.datastax.cdm.cql.CqlHelper;

import java.util.ArrayList;
import java.util.List;

public class BaseCdmStatement {

    protected PropertyHelper propertyHelper;
    protected CqlHelper cqlHelper;
    protected String statement = "";
    protected CqlSession session;

    protected List<String> resultColumns = new ArrayList<>();
    protected List<MigrateDataType> resultTypes = new ArrayList<>();
    protected UDTMapper udtMapper;
    protected boolean udtMappingEnabled;

    public BaseCdmStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper, CqlSession session) {
        this.propertyHelper = propertyHelper;
        this.cqlHelper = cqlHelper;
        this.udtMapper = (UDTMapper) cqlHelper.getFeature(Featureset.UDT_MAPPER);
        this.udtMappingEnabled = FeatureFactory.isEnabled(this.udtMapper);
        this.session = session;
    }

    public PreparedStatement prepareStatement() {
        if (null==session)
            throw new RuntimeException("Session is not set");
        if (null == statement || statement.isEmpty())
            throw new RuntimeException("Statement is not set");
        return session.prepare(statement);
    }

    public String getCQL() {
        return statement;
    }

}