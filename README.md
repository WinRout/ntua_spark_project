# ntua_spark_project
# About
This repository is dedicated to the semester project of the NTUA ECE course "Advanced Database Topics". 
In this project we use VMs to setup a spark cluster on top of hadoop. We used resources from "okeanos knossos" 
https://okeanos-knossos.grnet.gr/home/ GRNET's cloud service to setup 2 VMs. When the installation is done we
use the spark cluster to execute queries on high volume Dataframes and RDDs. More specifically, we used
parquet files and worked on taxi-trip data gathered from the New York's Taxi and Limousine Comission https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### setup
Contains instructions to setup the virtual machines, install hadoop and setup Spark cluster. (greek language)

### code
Contains the python script which it was used for submitting the jobs in the Spark cluster.

### outputs
Contains csv files correspinding to the results of each query.

### docs
Contains the report as well as the wording of the project. (greek language)

### time_outputs.csv
It shows the executed time of each query. The first part above represents one worker (2 cpus) in the Spark cluster. The below part represents two workers (4 cpus). Each time was measured as an average of 10 executes.
