package com.datastax.cdm.job

object DiffData extends BasePartitionJob {
  setup("Data Validation Job", new DiffJobSessionFactory())
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
