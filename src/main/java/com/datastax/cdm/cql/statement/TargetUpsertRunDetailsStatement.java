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

import com.datastax.cdm.feature.TrackRun;
import com.datastax.cdm.feature.TrackRun.RUN_TYPE;
import com.datastax.cdm.job.SplitPartitions;
import com.datastax.cdm.job.SplitPartitions.Partition;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;

public class TargetUpsertRunDetailsStatement {
	private CqlSession session;
	private String keyspaceName;
	private String tableName;
	private long runId;
	private long prevRunId;
	private BoundStatement boundInitInfoStatement;
	private BoundStatement boundInitStatement;
	private BoundStatement boundUpdateInfoStatement;
	private BoundStatement boundUpdateStatement;
	private BoundStatement boundUpdateStartStatement;
	private BoundStatement boundSelectStatement;

	public TargetUpsertRunDetailsStatement(CqlSession session, String keyspacetable) {
		this.session = session;
		String[] ksTab = keyspacetable.split("\\.");
		this.keyspaceName = ksTab[0];
		this.tableName = ksTab[1];
		String cdmKsTabInfo = this.keyspaceName + ".cdm_run_info";
		String cdmKsTabDetails = this.keyspaceName + ".cdm_run_details";

		this.session.execute("create table if not exists " + cdmKsTabInfo
				+ " (table_name text, run_id bigint, run_type text, prev_run_id bigint, start_time timestamp, end_time timestamp, run_info text, primary key (table_name, run_id))");
		this.session.execute("create table if not exists " + cdmKsTabDetails
				+ " (table_name text, run_id bigint, start_time timestamp, token_min bigint, token_max bigint, status text, primary key ((table_name, run_id), token_min))");

		boundInitInfoStatement = bindStatement("INSERT INTO " + cdmKsTabInfo
				+ " (table_name, run_id, run_type, prev_run_id, start_time) VALUES (?, ?, ?, ?, dateof(now()))");
		boundInitStatement = bindStatement("INSERT INTO " + cdmKsTabDetails
				+ " (table_name, run_id, token_min, token_max, status) VALUES (?, ?, ?, ?, ?)");
		boundUpdateInfoStatement = bindStatement("UPDATE " + cdmKsTabInfo
				+ " SET end_time = dateof(now()), run_info = ? WHERE table_name = ? AND run_id = ?");
		boundUpdateStatement = bindStatement(
				"UPDATE " + cdmKsTabDetails + " SET status = ? WHERE table_name = ? AND run_id = ? AND token_min = ?");
		boundUpdateStartStatement = bindStatement("UPDATE " + cdmKsTabDetails
				+ " SET start_time = dateof(now()), status = ? WHERE table_name = ? AND run_id = ? AND token_min = ?");
		boundSelectStatement = bindStatement("SELECT token_min, token_max FROM " + cdmKsTabDetails
				+ " WHERE table_name = ? AND run_id = ? and status in ('NOT_STARTED', 'STARTED', 'FAIL', 'DIFF') ALLOW FILTERING");
	}

	public Collection<SplitPartitions.Partition> getPendingPartitions(long prevRunId) {
		this.prevRunId = prevRunId;
		if (prevRunId == 0) {
			return new ArrayList<SplitPartitions.Partition>();
		}

		final Collection<SplitPartitions.Partition> pendingParts = new ArrayList<SplitPartitions.Partition>();
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
				.setString("run_type", runType.toString()).setLong("prev_run_id", prevRunId));
		parts.forEach(part -> initCdmRun(part));
		return runId;
	}

	private void initCdmRun(Partition partition) {
		session.execute(boundInitStatement.setString("table_name", tableName).setLong("run_id", runId)
				.setLong("token_min", partition.getMin().longValue())
				.setLong("token_max", partition.getMax().longValue())
				.setString("status", TrackRun.RUN_STATUS.NOT_STARTED.toString()));
	}

	public void updateCdmRunInfo(String runInfo) {
		session.execute(boundUpdateInfoStatement.setString("table_name", tableName).setLong("run_id", runId)
				.setString("run_info", runInfo));
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
