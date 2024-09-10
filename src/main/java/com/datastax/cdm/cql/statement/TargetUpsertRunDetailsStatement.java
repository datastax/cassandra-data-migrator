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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.feature.TrackRun.RUN_TYPE;
import com.datastax.cdm.job.RunNotStartedException;
import com.datastax.cdm.job.SplitPartitions;
import com.datastax.cdm.job.SplitPartitions.Partition;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class TargetUpsertRunDetailsStatement {
    private CqlSession session;
    private String keyspaceName;
    private String tableName;
    private long runId;
    private long prevRunId;
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

        this.session.execute("create table if not exists " + cdmKsTabInfo
                + " (table_name text, run_id bigint, run_type text, prev_run_id bigint, start_time timestamp, end_time timestamp, run_info text, status text, primary key (table_name, run_id))");

        // TODO: Remove this code block after a few releases, its only added for backward compatibility
        try {
            this.session.execute("alter table " + cdmKsTabInfo + " add status text");
        } catch (Exception e) {
            // ignore if column already exists
            logger.trace("Column 'status' already exists in table {}", cdmKsTabInfo);
        }

        boundInitInfoStatement = bindStatement("INSERT INTO " + cdmKsTabInfo
                + " (table_name, run_id, run_type, prev_run_id, start_time, status) VALUES (?, ?, ?, ?, dateof(now()), ?)");
        boundInitStatement = bindStatement("INSERT INTO " + cdmKsTabDetails
                + " (table_name, run_id, token_min, token_max, status) VALUES (?, ?, ?, ?, ?)");
        boundEndInfoStatement = bindStatement("UPDATE " + cdmKsTabInfo
                + " SET end_time = dateof(now()), run_info = ?, status = ? WHERE table_name = ? AND run_id = ?");
        boundUpdateStatement = bindStatement(
                "UPDATE " + cdmKsTabDetails + " SET status = ? WHERE table_name = ? AND run_id = ? AND token_min = ?");
        boundUpdateStartStatement = bindStatement("UPDATE " + cdmKsTabDetails
                + " SET start_time = dateof(now()), status = ? WHERE table_name = ? AND run_id = ? AND token_min = ?");
        boundSelectInfoStatement = bindStatement(
                "SELECT status FROM " + cdmKsTabInfo + " WHERE table_name = ? AND run_id = ?");
        boundSelectStatement = bindStatement("SELECT token_min, token_max FROM " + cdmKsTabDetails
                + " WHERE table_name = ? AND run_id = ? and status in ('NOT_STARTED', 'STARTED', 'FAIL', 'DIFF') ALLOW FILTERING");
    }

    public Collection<SplitPartitions.Partition> getPendingPartitions(long prevRunId) throws RunNotStartedException {
        this.prevRunId = prevRunId;
        final Collection<SplitPartitions.Partition> pendingParts = new ArrayList<SplitPartitions.Partition>();
        if (prevRunId == 0) {
            return pendingParts;
        }

        ResultSet rsInfo = session
                .execute(boundSelectInfoStatement.setString("table_name", tableName).setLong("run_id", prevRunId));
        Row cdmRunStatus = rsInfo.one();
        if (cdmRunStatus == null) {
            return pendingParts;
        } else {
            String status = cdmRunStatus.getString("status");
            if (TrackRun.RUN_STATUS.NOT_STARTED.toString().equals(status)) {
                throw new RunNotStartedException("Run not started for run_id: " + prevRunId);
            }
        }

        ResultSet rs = session
                .execute(boundSelectStatement.setString("table_name", tableName).setLong("run_id", prevRunId));
        rs.forEach(row -> {
            Partition part = new Partition(BigInteger.valueOf(row.getLong("token_min")),
                    BigInteger.valueOf(row.getLong("token_max")));
            pendingParts.add(part);
        });

        return pendingParts;
    }

    public long initCdmRun(Collection<SplitPartitions.Partition> parts, RUN_TYPE runType) {
        runId = System.currentTimeMillis();
        session.execute(boundInitInfoStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setString("run_type", runType.toString()).setLong("prev_run_id", prevRunId)
                .setString("status", TrackRun.RUN_STATUS.NOT_STARTED.toString()));
        parts.forEach(part -> initCdmRun(part));
        session.execute(boundInitInfoStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setString("run_type", runType.toString()).setLong("prev_run_id", prevRunId)
                .setString("status", TrackRun.RUN_STATUS.STARTED.toString()));
        return runId;
    }

    private void initCdmRun(Partition partition) {
        session.execute(boundInitStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setLong("token_min", partition.getMin().longValue())
                .setLong("token_max", partition.getMax().longValue())
                .setString("status", TrackRun.RUN_STATUS.NOT_STARTED.toString()));
    }

    public void endCdmRun(String runInfo) {
        session.execute(boundEndInfoStatement.setString("table_name", tableName).setLong("run_id", runId)
                .setString("run_info", runInfo).setString("status", TrackRun.RUN_STATUS.ENDED.toString()));
    }

    public void updateCdmRun(BigInteger min, TrackRun.RUN_STATUS status) {
        if (TrackRun.RUN_STATUS.STARTED.equals(status)) {
            session.execute(boundUpdateStartStatement.setString("table_name", tableName).setLong("run_id", runId)
                    .setLong("token_min", min.longValue()).setString("status", status.toString()));
        } else {
            session.execute(boundUpdateStatement.setString("table_name", tableName).setLong("run_id", runId)
                    .setLong("token_min", min.longValue()).setString("status", status.toString()));
        }
    }

    private BoundStatement bindStatement(String stmt) {
        if (null == session)
            throw new RuntimeException("Session is not set");
        return session.prepare(stmt).bind().setTimeout(Duration.ofSeconds(10));
    }

}
