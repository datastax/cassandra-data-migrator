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

import java.util
import scala.reflect.io.File
import com.datastax.cdm.feature.TrackRun
import com.datastax.cdm.properties.KnownProperties

abstract class BasePartitionJob extends BaseJob[SplitPartitions.Partition] {
  var trackRunFeature: TrackRun = _

  override def getParts(pieces: Int): util.Collection[SplitPartitions.Partition] = {
    var keyspacetable = propertyHelper.getString(KnownProperties.TARGET_KEYSPACE_TABLE)
  
    if (trackRun) {
      trackRunFeature = targetConnection.withSessionDo(targetSession => new TrackRun(targetSession, keyspacetable))
    }
    
    if (prevRunId != 0) {
      trackRunFeature.getPendingPartitions(prevRunId)
    } else if (!File(this.partitionFileNameInput).exists) {
      SplitPartitions.getRandomSubPartitions(pieces, minPartition, maxPartition, coveragePercent)
    } else {
      SplitPartitions.getSubPartitionsFromFile(pieces, this.partitionFileNameInput)
    }
  }

}