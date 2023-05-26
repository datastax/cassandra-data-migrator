package com.datastax.cdm.job

case class ConnectionDetails(
                              scbPath: String,
                              host: String,
                              port: String,
                              username: String,
                              password: String,
                              sslEnabled: String,
                              trustStorePath: String,
                              trustStorePassword: String,
                              trustStoreType: String,
                              keyStorePath: String,
                              keyStorePassword: String,
                              enabledAlgorithms: String
                            )