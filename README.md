# astra-spark-migration-ranges

Spark jobs in this repo can be used for data migration and data validation. 

 `Please note: this job has been tested with spark version 2.4.8`
 
Pre-requisite
 Install single instance of spark on a node where we want to run this job. Spark can be installed by running the following: -

wget https://downloads.apache.org/spark/spark-2.4.8/
tar -xvzf <spark downloaded file name>


Step1: 
SparkConf.txt file needs to be configured accordingly, for example below is the conf file for data migration job. 

Class name for data migration is `Migrate` 

Sample Spark conf file configuration for `Migrate` class can be found here: `astra-spark-migration-ranges/src/resources/sparkConf.properties`
 
 Example of the conf file is below: -

`spark.migrate.source.username               <Source cluster user_id>`
 <!-- blank line -->
`spark.migrate.source.password               <Source cluster password>`
 <!-- blank line -->
`spark.migrate.source.host                   <Source cluster host ip address or contact ip address>`
 <!-- blank line -->
`spark.migrate.astra.scb                     <path to scb for the target database on Astra>`
  <!-- blank line -->
`spark.migrate.astra.username                <Astra client_id from the token file>`
  <!-- blank line -->
`spark.migrate.astra.password                <Client_Secret from token file>`
  <!-- blank line -->
`spark.migrate.keyspaceName                  <keyspace name>`
  <!-- blank line -->
`spark.migrate.tableName                     <table name>`
  <!-- blank line -->
`spark.migrate.readRateLimit                 200000 <can be configured as needed>`
  <!-- blank line -->
`spark.migrate.writeRateLimit                200000 <can be configured as needed>`
  <!-- blank line -->
`spark.migrate.batchSize                     2 <batch size can be configured as needed>`
  <!-- blank line -->
`spark.migrate.source.writeTimeStampFilter   0`
  <!-- blank line -->

place the conf file where it can be accessed while runninn the job via spark-submit.
  
Step2: 
  
If the project is not already packaged, it needs to packaged via mvn. It will be generate a fat jar called `migrate-0.1.jar`

Once the jar file is ready, we can run the job via spark-submit command, below is spark submit for example: -
  ./spark-submit --properties-file /media/bulk/sparkConf.properties /
  --master "local[8]" /
  --conf spark.migrate.source.minPartition=-9223372036854775808 /
  --conf spark.migrate.source.maxPartition=9223372036854775807 /
  --class datastax.astra.migrate.Migrate /media/bulk/migrate-0.1.jar > <logfile_name>.txt
  
 
# Data-validation job
  
For Data validation same pre-requisite applies as Data migration. 
  Spark job needs a sparkConf file plus the `DataDiff` class while running spark submit.
  
Step1. Configure the sparkConf file as shown in example file `astra-spark-migration-ranges/src/resources/SparkConfDataValidation.txt`  
  


Step2.  If not, package the project via maven, it generates a jar file called `migrate-0.1.jar’  

Using this jar file we can run spark-submit command like below from bin directory of spark :- 

./spark-submit --properties-file ~/Downloads/spark-test/sparkConf.properties /
--verbose  / 
--master "local[8]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.DiffData /
~/Downloads/spark-test/migrate-0.1.jar    

Additionally you could write the output to a log file like “log_tablename” to avoid getting the output on the console. 


Step3.	On the output of the run, job will report “ERRORS” like below reporting data differences. 
22/01/14 10:17:10 ERROR CopyJobSession: Data difference found -  Key: 5b6962dd-3f90-4c93-8f61-eabfa4a803e5 Data:  (Index: 1 Source: Anoop Astra: AA )  (Index: 2 Source: T Astra: TIW ) 
22/
 
22/01/14 10:17:10 INFO CopyJobSession: TreadID: 56 Final Differences Count: 1
22/01/14 10:17:10 INFO CopyJobSession: TreadID: 56 Final Valid Count: 1
22/01/14 10:17:10 INFO Executor: Finished task 3.0 in stage 0.0 (TID 3). 751 bytes result sent to driver
22/01/14 10:17:10 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 4541 ms on localhost (executor driver) (3/6)
22/01/14 10:17:10 ERROR CopyJobSession: Data is missing in Astra: e7cd5752-bc0d-4157-a80f-7523add8dbcd
22/01/14 10:17:10 ERROR CopyJobSession: Data is missing in Astra: 5b6962dd-3f90-4c93-8f61-eabfa4a803e3

Step4.	Please grep for all ERROR from the output log files to get the list of differences, notice that its listing differences by partition key value in this case.  
