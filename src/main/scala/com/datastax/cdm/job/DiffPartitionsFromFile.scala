package com.datastax.cdm.job

object DiffPartitionsFromFile extends BasePartitionJob {
  setup("Diff Partitions from File Job", new DiffJobSessionFactory())
  if ("".equals(this.fileName)) {
    abstractLogger.error("Please set conf for spark.input.partitionFile ")
  }
  else {
    execute()
    finish()
  }

  override def execute(): Unit = {
    slices.foreach(slice => {
      originConnection.withSessionDo(sourceSession =>
        targetConnection.withSessionDo(destinationSession =>
          jobFactory.getInstance(sourceSession, destinationSession, sc)
            .processSlice(slice)))
    })
  }
}



