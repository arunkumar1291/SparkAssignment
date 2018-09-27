# SparkAssignment

Prerequisites :
---------------

Multinode cluster( Minimum 3 nodes ) of Hdfs, Spark and Hbase.


Steps to run the application :
-------------------------------

* Start the services of hdfs, spark and hbase.
* Go to the terminal and do sudo git clone "githuburl"
* Go into the git hub project folder and run "mvn package install"
* SparkAssignment-0.0.1-SNAPSHOT.jar will be created under target folder.
* There are two spark applications available in the same jar file, one is to 
  combine all the data based on the QID from both asset/jsonfiles and VULN/jsonfiles and store them in unified format (Parquet)
  and second application is for generating the reports.
  
* To combine the asset and VULN data, run the below spark submit command 

> spark-submit --class com.risksense.CombineData --master yourclustermasterurl --total-executor-cores 9 --executor-memory 2G  --num-executors 54 /path/to/SparkAssignment-0.0.1-SNAPSHOT.jar path/to/asset/jsonfiles path/to/vuln/jsonfiles hdfs/path/for/storing/the/combined/data

* The above command will create the data combined both asset and vuln in the parquet format. 

* To generate the reports, run the below spark submit command 

> spark-submit --class com.risksense.ReadParquetData --master yourclustermasterurl --total-executor-cores 9 --executor-memory 2G  --num-executors 54 /path/to/SparkAssignment-0.0.1-SNAPSHOT.jar comma,separated,ipsofclusternodes hbaseMasterIp:port  /hdfs/path/which/has/the/combined/data

* Once the applications are submitted successfully, you can check the tables in hbase shell for each and every report.

Reports and Tables:
-------------------
Number of records available for each ip = no_of_records
List of ips associated with each unique port = port_level_ips
List of ips associated with each qunique CVE.ID	= cve_ips
Count of ips associated with each unique SEVERITY_LEVEL for each dateloaded.$date = severity_level_ips
