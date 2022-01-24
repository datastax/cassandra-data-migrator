# astra-spark-migration-ranges

Spark jobs in this repo can be used for data migration and data validation. 

# `Please note: this job has been tested with spark version 2.4.8`

# Step1: 
SparkConf.txt file needs to be configured accordingly, for example below is the conf file for data migration job. 

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

place the confile where it can be accessed while runninn the job via spark-submit.
  
# Step2: 
  
If the project is not already packaged, it needs to packaged via mvn. It will be generate a fat jar called `migrate-0.1.jar`

Once the jar file is ready, we can run the job via spark-submit command, below is spark submit for example: -
  ./spark-submit --properties-file /media/bulk/sparkConf.properties /
  --master "local[8]" /
  --conf spark.migrate.source.minPartition=-9223372036854775808 /
  --conf spark.migrate.source.maxPartition=9223372036854775807 /
  --class datastax.astra.migrate.Migrate /media/bulk/migrate-0.1.jar > <logfile_name>.txt
  
  
