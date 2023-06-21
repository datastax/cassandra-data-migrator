package com.datastax.cdm.job

import java.util

abstract class BasePKJob extends BaseJob[SplitPartitions.PKRows] {
  override def getParts(pieces: Int): util.Collection[SplitPartitions.PKRows] = {
    // This takes a file with N rows and divides it into pieces of size N/pieces
    // Each PKRows object contains a list of Strings that contain the PK to be parsed
    SplitPartitions.getRowPartsFromFile(pieces, this.partitionFileName)
  }
}