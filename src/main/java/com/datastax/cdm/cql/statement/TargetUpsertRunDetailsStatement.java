/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.cql.statement;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.job.IJobSessionFactory.JobType;
import com.datastax.cdm.job.PartitionRange;
import com.datastax.cdm.job.RunNotStartedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class TargetUpsertRunDetailsStatement {
    private CqlSession session;
    private String keyspaceName;
    private String tableName;
    private BoundStatement boundInitInfoStatement;
    private BoundStatement boundInitStatement;
    private BoundStatement boundEndInfoStatement;
    private BoundStatement boundUpdateStatement;
    private BoundStatement boundUpdateStartStatement;
    private BoundStatement boundSelectInfoStatement;
    private BoundStatement boundSelectStatement;

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public TargetUpsertRunDetailsStatement(CqlSession session, String keyspaceTable) {
        this.session = session;
        String[] ksTab = keyspaceTable.split("\\.");
        if (ksTab.length != 2) {
            throw new RuntimeException("Invalid keyspace.table format: " + keyspaceTable);
        }
        this.keyspaceName = ksTab[0];
        this.tableName = ksTab[1];
        String cdmKsTabInfo = this.keyspaceName + ".cdm_run_info";
        String cdmKsTabDetails = this.keyspaceName + ".cdm_run_details";

        this.session.execute("CREATE TABLE IF NOT EXISTS " + cdmKsTabInfo
                + " (table_name TEXT, run_id BIGINT, run_type TEXT, prev_run_id BIGINT, start_time TIMESTAMP, end_time TIMESTAMP, run_info TEXT, status TEXT, PRIMARY KEY (table_name, run_id))");
        this.session.execute("CREATE TABLE IF NOT EXISTS " + cdmKsTabDetails
                + " (table_name TEXT, run_id BIGINT, start_time TIMESTAMP, token_min BIGINT, token_max BIGINT, status TEXT, run_info TEXT, PRIMARY KEY ((table_name, run_id), token_min))");

        // TODO: Remove this code block after a few releases, its only added for backward compatibility
        try {
            this.session.execute("ALTER TABLE " + cdmKsTabInfo + " ADD status TEXT");
        } catch (Exception e) { // ignore if column already exists
            logger.debug("Column 'status' already exists in table {}", cdmKsTabInfo);
        }
        try {
            this.session.execute("ALTER TABLE " + cdmKsTabDetails + " ADD run_info TEXT");
        } catch (Exception e) { // ignore if column already exists
            logger.debug("Column 'run_info' already exists in table {}", cdmKsTabDetails);
        }

        boundInitInfoStatement = bindStatement("INSERT INTO " + cdmKsTabInfo
                + " (table_name, run_id, run_type, prev_run_id, start_time, status) VALUES (?, ?, ?, ?, totimestamp(now()), ?)");
        boundInitStatement = bindStatement("INSERT INTO " + cdmKsTabDetails
                + " (table_name, run_id, token_min, token_max, status) VALUES (?, ?, ?, ?, ?)");
        boundEndInfoStatement = bindStatement("UPDATE " + cdmKsTabInfo
                + " SET end_time = totimestamp(now()), run_info = ?, status = ? WHERE table_name = ? AND run_id = ?");
        boundUpdateStatement = bindStatement("UPDATE " + cdmKsTabDetails
                + " SET status = ?, run_info = ? WHERE table_name = ? AND run_id = ? AND token_min = ?");
        boundUpdateStartStatement = bindStatement("UPDATE " + cdmKsTabDetails
                + " SET start_time = totimestamp(now()), status = ? WHERE table_name = ? AND run_id = ? AND token_min = ?");
        boundSelectInfoStatement = bindStatement(
                "SELECT status FROM " + cdmKsTabInfo + " WHERE table_name = ? AND run_id = ?");
        boundSelectStatement = bindStatement("SELECT token_min, token_max FROM " + cdmKsTabDetails
                + " WHERE table_name = ? AND run_id = ? AND status = ? ALLOW FILTERING");
    }

    public Collection<PartitionRange> getPendingPartitions(long prevRunId, JobType jobType)
            throws RunNotStartedException {
        if (prevRunId == 0) {
            return Collections.emptyList();
        }

        ResultSet rsInfo = session
                .execute(boundSelectInfoStatement.setString("table_name", tableName).setLong("run_id", prevRunId));
        Row cdmRunStatus = rsInfo.one();
        if (cdmRunStatus == null) {
            throw new RunNotStartedException(
                    "###################### Run NOT FOUND for Previous RunId: " + prevRunId + ", starting new run!");
        } else {
            String status = cdmRunStatus.getString("status");
            if (TrackRun.RUN_STATUS.NOT_STARTED.toString().equals(status)) {
                throw new RunNotStartedException("###################### Run NOT STARTED for Previous RunId: "
                        + prevRunId + ", starting new run!");
            }
        }

        final List<PartitionRange> pendingParts = new ArrayList<PartitionRange>();
        // Use an array of statuses for iteration
        String[] statuses = { TrackRun.RUN_STATUS.NOT_STARTED.toString(), TrackRun.RUN_STATUS.STARTED.toString(),
                TrackRun.RUN_STATUS.FAIL.toString(), TrackRun.RUN_STATUS.DIFF.toString() };
        for (String status : statuses) {
            pendingParts.addAll(getPartitionsByStatus(prevRunId, status, jobType));
        }
        Collections.shuffle(pendingParts);
        Collections.shuffle(pendingParts);

        return pendingParts;
    }

    protected Collection<PartitionRange> getPartitionsByStatus(long runId, String status, JobType jobType) {
        final Collection<PartitionRange> pendingParts = new ArrayList<PartitionRange>();
        getResultSetByStatus(runId, status).forEach(row -> {
            PartitionRange part = new PartitionRange(BigInteger.valueOf(row.getLong("token_min")),
                    BigInteger.valueOf(row.getLong("token_max")), jobType);
            pendingParts.add(part);
        });
        return pendingParts;
    }

    protected ResultSet getResultSetByStatus(long runId, String status) {
        return session.execute(boundSelectStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setString("status", status));
    }

    public void initCdmRun(long runId, long prevRunId, Collection<PartitionRange> parts, JobType jobType) {
        ResultSet rsInfo = session
                .execute(boundSelectInfoStatement.setString("table_name", tableName).setLong("run_id", runId));
        if (null != rsInfo.one()) {
            throw new RuntimeException("Run id " + runId + " already exists for table " + tableName);
        }
        session.execute(boundInitInfoStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setString("run_type", jobType.toString()).setLong("prev_run_id", prevRunId)
                .setString("status", TrackRun.RUN_STATUS.NOT_STARTED.toString()));
        parts.forEach(part -> initCdmRun(runId, part));
        session.execute(boundInitInfoStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setString("run_type", jobType.toString()).setLong("prev_run_id", prevRunId)
                .setString("status", TrackRun.RUN_STATUS.STARTED.toString()));
    }

    private void initCdmRun(long runId, PartitionRange partition) {
        session.execute(boundInitStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setLong("token_min", partition.getMin().longValue())
                .setLong("token_max", partition.getMax().longValue())
                .setString("status", TrackRun.RUN_STATUS.NOT_STARTED.toString()));
    }

    public void endCdmRun(long runId, String runInfo) {
        session.execute(boundEndInfoStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setString("run_info", runInfo).setString("status", TrackRun.RUN_STATUS.ENDED.toString()));
    }

    public void updateCdmRun(long runId, BigInteger min, TrackRun.RUN_STATUS status, String runInfo) {
        if (TrackRun.RUN_STATUS.STARTED.equals(status)) {
            session.execute(boundUpdateStartStatement.setString("table_name", tableName).setLong("run_id", runId)
                    .setLong("token_min", min.longValue()).setString("status", status.toString()));
        } else {
            session.execute(boundUpdateStatement.setString("table_name", tableName).setLong("run_id", runId)
                    .setLong("token_min", min.longValue()).setString("status", status.toString())
                    .setString("run_info", runInfo));
        }
    }

    private BoundStatement bindStatement(String stmt) {
        if (null == session)
            throw new RuntimeException("Session is not set");
        return session.prepare(stmt).bind().setTimeout(Duration.ofSeconds(10));
    }

}
