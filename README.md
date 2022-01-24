# astra-spark-migration-ranges

Spark jobs in this repo can be used for data migration and data validation. 
'Please note: this job has been tested with spark version 2.4.8`

SparkConf.txt file needs to be configured accordingle, for example below is the conf file for data migration job. 

Class name for data migration is `Migrate` 

#Spark conf file configuration for `Migrate` class

spark.migrate.source.username               <Source cluster user_id>
spark.migrate.source.password               <Source cluster password>
spark.migrate.source.host                   <Source cluster host ip address or contact ip address>

spark.migrate.astra.scb                     <path to scb for the target database on Astra>
spark.migrate.astra.username                <Astra client_id from the token file>
spark.migrate.astra.password                <Client_Secret from token file>
spark.migrate.keyspaceName                  <keyspace name>
spark.migrate.tableName                     <table name> 
spark.migrate.readRateLimit                 200000 <can be configured as needed>
spark.migrate.writeRateLimit                200000 <can be configured as needed>
spark.migrate.batchSize                     2 <batch size can be configured as needed>
spark.migrate.source.writeTimeStampFilter   0
