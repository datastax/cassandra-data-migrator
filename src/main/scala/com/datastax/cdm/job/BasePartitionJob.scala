package com.datastax.cdm.job

import java.util
import scala.reflect.io.File

abstract class BasePartitionJob extends BaseJob[SplitPartitions.Partition] {
  override def getParts(pieces: Int): util.Collection[SplitPartitions.Partition] = {
    if (!File(this.partitionFileName).exists) {
      SplitPartitions.getRandomSubPartitions(pieces, minPartition, maxPartition, coveragePercent)
    } else {
      SplitPartitions.getSubPartitionsFromFile(pieces, this.partitionFileName)
    }
  }

}