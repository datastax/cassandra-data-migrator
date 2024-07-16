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

import com.datastax.cdm.job.SplitPartitions;
import com.datastax.cdm.job.SplitPartitions.Partition;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;

public class TargetUpsertRunDetailsStatement {
	private CqlSession session;
	private String keyspaceName;
	private String tableName;
	private long runId;
	private long prevRunId;
	private BoundStatement boundInitStatement;
	private BoundStatement boundUpdateStatement;
	private BoundStatement boundSelectStatement;

	public TargetUpsertRunDetailsStatement(CqlSession session, String keyspacetable) {
		this.session = session;
		String[] ksTab = keyspacetable.split("\\.");
		this.keyspaceName = ksTab[0];
		this.tableName = ksTab[1];
		String cdmKsTab = this.keyspaceName + ".cdm_run_details";

		String createStmt = "create table if not exists " + cdmKsTab
				+ " (table_name text, run_id bigint, token_min bigint, token_max bigint, prev_run_id bigint, status text, primary key ((table_name, run_id), token_min))";
		this.session.execute(createStmt);

		String initStmt = "INSERT INTO " + cdmKsTab
				+ " (table_name, run_id, token_min, token_max, prev_run_id, status) VALUES (?, ?, ?, ?, ?, ?)";
		boundInitStatement = prepareStatement(initStmt).bind();

		String updateStmt = "UPDATE " + cdmKsTab
				+ " SET status = ? WHERE table_name = ? AND run_id = ? AND token_min = ?";
		boundUpdateStatement = prepareStatement(updateStmt).bind();

		String selectStmt = "SELECT token_min, token_max FROM " + cdmKsTab
				+ " WHERE table_name = ? AND run_id = ? and status in ('NOT STARTED', 'STARTED', 'FAIL') ALLOW FILTERING";
		boundSelectStatement = prepareStatement(selectStmt).bind();
	}

	public Collection<SplitPartitions.Partition> getPendingPartitions(long prevRunId) {
		this.prevRunId = prevRunId;
		if (prevRunId == 0) {
			return new ArrayList<SplitPartitions.Partition>();
		}

		final Collection<SplitPartitions.Partition> pendingParts = new ArrayList<SplitPartitions.Partition>();
		ResultSet rs = session.execute(boundSelectStatement.setString("table_name", tableName)
				.setLong("run_id", prevRunId).setTimeout(Duration.ofSeconds(10)));
		rs.forEach(row -> {
			Partition part = new Partition(BigInteger.valueOf(row.getLong("token_min")),
					BigInteger.valueOf(row.getLong("token_max")));
			pendingParts.add(part);
		});

		return pendingParts;
	}

	public long initCdmRun(Collection<SplitPartitions.Partition> parts) {
		runId = System.currentTimeMillis();
		parts.forEach(part -> initCdmRun(part));
		return runId;
	}

	private void initCdmRun(Partition partition) {
		session.execute(boundInitStatement.setString("table_name", tableName).setLong("run_id", runId)
				.setLong("token_min", partition.getMin().longValue())
				.setLong("token_max", partition.getMax().longValue()).setString("status", "NOT STARTED")
				.setLong("prev_run_id", prevRunId).setTimeout(Duration.ofSeconds(10)));
	}

	public void updateCdmRun(BigInteger min, String status) {
		session.execute(boundUpdateStatement.setString("table_name", tableName).setLong("run_id", runId)
				.setLong("token_min", min.longValue()).setString("status", status).setTimeout(Duration.ofSeconds(10)));
	}

	private PreparedStatement prepareStatement(String stmt) {
		if (null == session)
			throw new RuntimeException("Session is not set");
		return session.prepare(stmt);
	}

}
