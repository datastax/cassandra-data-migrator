package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import datastax.cdm.feature.FeatureFactory;
import datastax.cdm.feature.Featureset;
import datastax.cdm.feature.UDTMapper;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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