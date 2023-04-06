package datastax.cdm.cql.statement;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import datastax.cdm.job.MigrateDataType;
import datastax.cdm.cql.CqlHelper;
import datastax.cdm.properties.PropertyHelper;

import java.util.ArrayList;
import java.util.List;

public class BaseCdmStatement {

    protected PropertyHelper propertyHelper;
    protected CqlHelper cqlHelper;
    protected String statement = "";
    protected CqlSession session;

    protected List<String> resultColumns = new ArrayList<>();
    protected List<MigrateDataType> resultTypes = new ArrayList<>();

    public BaseCdmStatement(PropertyHelper propertyHelper, CqlHelper cqlHelper) {
        this.propertyHelper = propertyHelper;
        this.cqlHelper = cqlHelper;
    }

    public PreparedStatement prepareStatement() {
        if (null == session || session.isClosed())
            throw new RuntimeException("Session is not ready for use, session=" + session);
        if (null == statement || statement.isEmpty())
            throw new RuntimeException("Statement is not set");
        return session.prepare(statement);
    }

    public String getCQL() {
        return statement;
    }

}