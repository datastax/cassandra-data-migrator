package com.datastax.cdm.job

object MigratePartitionsFromFile extends BasePartitionJob {
  setup("Migrate Partitions from File Job", new CopyJobSessionFactory(), "./partitions.csv")
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



