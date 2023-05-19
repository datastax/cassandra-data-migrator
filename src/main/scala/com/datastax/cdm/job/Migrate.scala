package com.datastax.cdm.job

object Migrate extends BasePartitionJob {
  setup("Migrate Job", new CopyJobSessionFactory())
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



