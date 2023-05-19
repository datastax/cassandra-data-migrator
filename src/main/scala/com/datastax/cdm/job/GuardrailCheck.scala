package com.datastax.cdm.job

object GuardrailCheck extends BasePartitionJob {
  setup("Guardrail Check Job", new GuardrailCheckJobSessionFactory())
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

