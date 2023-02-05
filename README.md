# ntua_spark_project
# About
This repository is dedicated to the semester project of the NTUA ECE course "Advanced Database Topics". 
In this project we use VMs to setup a spark cluster on top of hadoop. We used resources from "okeanos knossos" 
https://okeanos-knossos.grnet.gr/home/ GRNET's cloud service to setup 2 VMs. When the installation is done we
use the spark cluster to execute queries on high volume data using SQL/Dataframe API and RDD API. More specifically, we used
parquet files and worked on taxi-trip data gathered from the New York's Taxi and Limousine Comission https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### setup
Contains instructions to setup the virtual machines, install hadoop and setup Spark cluster.

### code
Contains the python script which it was used for submitting the jobs in the Spark cluster. It executes 5 queries and writes the results in csv files which can be found in the outputs folder. It also records the execution time of each and saves it to time_outputs.csv file.

### outputs
Contains csv files correspinding to the results of each query.

### docs
Contains the report as well as the wording of the project. 

### time_outputs.csv
It shows the execution time of each query. The rows above correspond to one worker (2 cpus) in the Spark cluster. The rows below correspond to two workers (4 cpus). Each time was measured as an average of 10 executes.
