package com.datastax.cdm.job

object DiffPartitionsFromFile extends BasePartitionJob {
  setup("Diff Partitions from File Job", new DiffJobSessionFactory(), tokenRangeFile)
  execute()
  finish()

  override def execute(): Unit = {
    slices.foreach(slice => {
      originConnection.withSessionDo(sourceSession =>
        targetConnection.withSessionDo(destinationSession =>
          jobFactory.getInstance(sourceSession, destinationSession, sc)
            .processSlice(slice)))
    })
  }
}



