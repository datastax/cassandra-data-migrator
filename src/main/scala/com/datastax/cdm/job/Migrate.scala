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
package com.datastax.cdm.job

import com.datastax.cdm.feature.TrackRun
import com.datastax.cdm.job.CDMMetricsAccumulator
import com.datastax.cdm.data.PKFactory.Side
import com.datastax.cdm.properties.KnownProperties
import com.datastax.cdm.job.IJobSessionFactory.JobType

object Migrate extends BasePartitionJob {
  jobType = JobType.MIGRATE
  setup("Migrate Job", new CopyJobSessionFactory())
  execute()
  finish()
  
  protected def execute(): Unit = {
    if (!parts.isEmpty()) {
      originConnection.withSessionDo(originSession =>
        targetConnection.withSessionDo(targetSession => 
          jobFactory.getInstance(originSession, targetSession, propertyHelper).initCdmRun(runId, prevRunId, parts, trackRunFeature, jobType)))
      var ma = new CDMMetricsAccumulator(jobType)
      sContext.register(ma, "CDMMetricsAccumulator")
      
      val bcOriginConfig = sContext.broadcast(sContext.getConf)
      val bcTargetConfig = sContext.broadcast(sContext.getConf)
      val bcConnectionFetcher = sContext.broadcast(connectionFetcher)
      val bcPropHelper = sContext.broadcast(propertyHelper)
      val bcJobFactory = sContext.broadcast(jobFactory)
      val bcKeyspaceTableValue = sContext.broadcast(keyspaceTableValue)
      val bcRunId = sContext.broadcast(runId)

      slices.foreach(slice => {
        if (null == originConnection) {
    		originConnection = bcConnectionFetcher.value.getConnection(bcOriginConfig.value, Side.ORIGIN, bcPropHelper.value.getString(KnownProperties.READ_CL), bcRunId.value)
    		targetConnection = bcConnectionFetcher.value.getConnection(bcTargetConfig.value, Side.TARGET, bcPropHelper.value.getString(KnownProperties.READ_CL), bcRunId.value)
            trackRunFeature = targetConnection.withSessionDo(targetSession => new TrackRun(targetSession, bcKeyspaceTableValue.value))
        }
        originConnection.withSessionDo(originSession =>
          targetConnection.withSessionDo(targetSession => {
            bcJobFactory.value.getInstance(originSession, targetSession, bcPropHelper.value)
              .processPartitionRange(slice, trackRunFeature, bcRunId.value)
              ma.add(slice.getJobCounter())
        }))
      })
      
      ma.value.printMetrics(runId, trackRunFeature);
    }
  }
}
