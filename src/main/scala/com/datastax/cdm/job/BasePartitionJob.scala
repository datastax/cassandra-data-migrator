package com.datastax.cdm.job

import java.util

abstract class BasePartitionJob extends BaseJob[SplitPartitions.Partition] {
  override def getParts(pieces: Int): util.Collection[SplitPartitions.Partition] = {
    if ("".equals(this.fileName)) {
      abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)
      SplitPartitions.getRandomSubPartitions(pieces, minPartition, maxPartition, coveragePercent)
    } else {
      SplitPartitions.getSubPartitionsFromFile(pieces, this.fileName)
    }
  }
}