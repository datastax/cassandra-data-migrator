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

import com.datastax.cdm.cql.EnhancedSession;
import com.datastax.cdm.job.SplitPartitions.Partition;
import com.datastax.cdm.properties.IPropertyHelper;
import com.datastax.cdm.properties.KnownProperties;
import com.datastax.cdm.schema.CqlTable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class TargetUpsertRunDetailsStatement {
	protected IPropertyHelper propertyHelper;
	protected CqlTable cqlTable;
	protected EnhancedSession session;
	private long runId = System.currentTimeMillis();
	private BoundStatement boundInitStatement;
	private BoundStatement boundUpdateStatement;

	public TargetUpsertRunDetailsStatement(IPropertyHelper propertyHelper, EnhancedSession session) {
		if (null == propertyHelper || null == session || null == session.getCqlTable())
			throw new RuntimeException("PropertyHelper or EnhancedSession or EnhancedSession.getCqlTable() is not set");
		this.propertyHelper = propertyHelper;
		this.cqlTable = session.getCqlTable();
		this.session = session;

		String initStmt = "INSERT INTO " + cqlTable.getRunDetailTable()
				+ " (table_name, run_id, token_min, token_max, prev_run_id) VALUES (?, ?, ?, ?, ?)";
		boundInitStatement = prepareStatement(initStmt).bind();

		String updateStmt = "UPDATE " + cqlTable.getRunDetailTable()
				+ " SET status = ? WHERE table_name = ? AND run_id = ? AND token_min = ?";
		boundUpdateStatement = prepareStatement(updateStmt).bind();
	}

	public void initCdmRun(Partition partition) {
		BoundStatement boundStatement = boundInitStatement.setString("table_name", cqlTable.getTableName())
				.setLong("run_id", runId).setDouble("token_min", partition.getMin().doubleValue())
				.setDouble("token_max", partition.getMax().doubleValue()).setLong("prev_run_id", getPreviousRunId())
				.setConsistencyLevel(cqlTable.getWriteConsistencyLevel()).setTimeout(Duration.ofSeconds(10));

		session.getCqlSession().execute(boundStatement);
	}

	public void updateCdmRun(BigInteger min, boolean status) {
		BoundStatement boundStatement = boundUpdateStatement.setString("table_name", cqlTable.getTableName())
				.setLong("run_id", runId).setDouble("token_min", min.doubleValue()).setBoolean("status", status)
				.setConsistencyLevel(cqlTable.getWriteConsistencyLevel()).setTimeout(Duration.ofSeconds(10));

		session.getCqlSession().execute(boundStatement);
	}

	private long getPreviousRunId() {
		long prevRunId = propertyHelper.getLong(KnownProperties.PREV_RUN_ID);
		if (prevRunId == 0) {
			prevRunId = runId;
		}

		return prevRunId;
	}

	public PreparedStatement prepareStatement(String stmt) {
		if (null == session || null == session.getCqlSession())
			throw new RuntimeException("Session is not set");
		return session.getCqlSession().prepare(stmt);
	}

}
