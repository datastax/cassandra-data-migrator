package com.datastax.cdm.job

object MigrateRowsFromFile extends BasePKJob {
  setup("Migrate Rows from File Job", new CopyPKJobSessionFactory(), "./primary_key_rows.csv")
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
