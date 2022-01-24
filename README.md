# astra-spark-migration-ranges

Spark jobs in this repo can be used for data migration and data validation. 

 `Please note: this job has been tested with spark version 2.4.8`
 
Pre-requisite
 Install single instance of spark on a node where we want to run this job. Spark can be installed by running the following: -

wget https://downloads.apache.org/spark/spark-2.4.8/
tar -xvzf <spark downloaded file name>


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
  
 
#Data-validation job
  
For Data validation same pre-requisite applies as Data migration. 
  Spark job needs a sparkConf file plus the `DataDiff` class while running spark submit.
  
 Step1: Configure the sparkConf file as below. 
  
  
spark.migrate.beta                                             False // this should be false if origin cluster is on-prem and Astra is used.

spark.migrate.source.host                                       <Source cluster contact_ip address>
spark.migrate.source.username                             <Source username>
spark.migrate.source.password                               <Source password>


spark.migrate.astra.scb                                      <path to scb for Astra> 
spark.migrate.astra.username                                    <Client_id from token file>
spark.migrate.astra.password                                    < <Client_secret from token file>>

spark.cassandra.source.read.consistency.level                   LOCAL_QUORUM
spark.cassandra.astra.read.consistency.level                    LOCAL_QUORUM

// no changes needed for below 5 lines. Readrate limit can be increased to make it faster but depends on overall load of the cluster.
spark.migrate.maxRetries                                        10
spark.migrate.readRateLimit                                     40000
spark.migrate.writeRateLimit                                    40000
spark.migrate.splitSize                                         5
spark.migrate.batchSize                                         5


spark.migrate.source.keyspaceTable                             <source keyspace.tablename>
spark.migrate.astra.keyspaceTable                               <astra keyspace.tablename>


spark.migrate.query.cols.select                                 primary key,col1,col2,col3,col4….all the columns of the table.
spark.migrate.diff.select.types                                 9,0 (data type of the columns, please refer to the end of this doc)
spark.migrate.query.cols.id                                     key,key2 (this is primary key which is Partition key, Clustering key)
spark.migrate.query.cols.id.types                               9 <data types of the primary key columns for example 0,0,1>
spark.migrate.query.cols.partitionKey                           key  ( this is just the partition key column)


//no changes needed in below params unless timestamp filtering, TTL is needed. 
spark.migrate.query.cols.insert
spark.migrate.query.cols.insert.types                           0
spark.migrate.source.counterTable                               false
spark.migrate.source.counterTable.update.cql
spark.migrate.source.counterTable.update.max.counter.index      0
spark.migrate.source.counterTable.update.select.index           0
spark.migrate.preserveTTLWriteTime                              false
spark.migrate.source.ttl.cols                                   10
spark.migrate.source.writeTimeStampFilter.cols                  11
spark.migrate.source.writeTimeStampFilter                       0
spark.migrate.source.maxWriteTimeStampFilter                    0

//Following are corresponding values for data types
/*
0: String

1: Integer

2: Long
3: Double
4: Instant (datetime)
5: Map (separate type by %) - Example: 5%0%1 (Map<String,Integer>)
6: List (separate type by %) - Example: 5%1 (List<Long>)
7: ByteBuffer (Blob)
8: Set (seperate type by %) - Example: 5%1 (Set<Long>)
9: UUID

*/


	2.	If not, package the project via maven, it generates a jar file called `migrate-0.1.jar’ 

Using this jar file we can run spark-submit command like below from bin directory of spark :- 

./spark-submit --properties-file ~/Downloads/spark-test/sparkConf.properties /
--verbose  / 
--master "local[8]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.DiffData /
~/Downloads/spark-test/migrate-0.1.jar    

Additionally you could write the output to a log file like “log_tablename” to avoid getting the output on the console. 


	3.	On the output of the run, job will report “ERRORS” like below reporting data differences. 
22/01/14 10:17:10 ERROR CopyJobSession: Data difference found -  Key: 5b6962dd-3f90-4c93-8f61-eabfa4a803e5 Data:  (Index: 1 Source: Anoop Astra: AA )  (Index: 2 Source: T Astra: TIW ) 
22/
 
22/01/14 10:17:10 INFO CopyJobSession: TreadID: 56 Final Differences Count: 1
22/01/14 10:17:10 INFO CopyJobSession: TreadID: 56 Final Valid Count: 1
22/01/14 10:17:10 INFO Executor: Finished task 3.0 in stage 0.0 (TID 3). 751 bytes result sent to driver
22/01/14 10:17:10 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 4541 ms on localhost (executor driver) (3/6)
22/01/14 10:17:10 ERROR CopyJobSession: Data is missing in Astra: e7cd5752-bc0d-4157-a80f-7523add8dbcd
22/01/14 10:17:10 ERROR CopyJobSession: Data is missing in Astra: 5b6962dd-3f90-4c93-8f61-eabfa4a803e3

	4.	Please grep for all ERROR from the output log files to get the list of differences, notice that its listing differences by partition key value in this case. 
