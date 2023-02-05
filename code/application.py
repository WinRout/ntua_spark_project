import findspark
import time, csv
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from functools import reduce
from pyspark.sql.functions import *
from pyspark import SparkContext

findspark.init() 
sc =SparkContext()
spark = SparkSession.builder.appName("Semester Project").getOrCreate()

#Create yellow_tripdata Dataframe for each month
x1 = spark.read.parquet("hdfs://master:9000/user/user/parquets/yellow_tripdata_2022-01.parquet")
x2 = spark.read.parquet("hdfs://master:9000/user/user/parquets/yellow_tripdata_2022-02.parquet")
x3 = spark.read.parquet("hdfs://master:9000/user/user/parquets/yellow_tripdata_2022-03.parquet")
x4 = spark.read.parquet("hdfs://master:9000/user/user/parquets/yellow_tripdata_2022-04.parquet")
x5 = spark.read.parquet("hdfs://master:9000/user/user/parquets/yellow_tripdata_2022-05.parquet")
x6 = spark.read.parquet("hdfs://master:9000/user/user/parquets/yellow_tripdata_2022-06.parquet")

#merge tripdata months into one DataFrame
dfs = [x1,x2,x3,x4,x5,x6]
tripdata = reduce(DataFrame.unionAll, dfs)

#get rid-off false data 
tripdata = tripdata.where(tripdata.tpep_pickup_datetime >= "2022-01-01").where(tripdata.tpep_pickup_datetime < "2022-07-01")
tripdata.show()

#Create lookup DataFrame
lookup = spark.read.load("hdfs://master:9000/user/user/lookup.csv", 
format="csv", inferSchema="true", header="true")
lookup.show()

#Create RDDs
tripdata_rdd = tripdata.rdd
lookup_rdd = lookup.rdd

#Create Temporary Table of tripdata in order to use SQL Queries
tripdata.registerTempTable("tripdata_table")
lookup.registerTempTable("lookup_table")

#SQL Queries
query1 = "SELECT * \
          FROM tripdata_table \
          WHERE DOLocationID = \
            (SELECT LocationID \
             FROM lookup_table \
             WHERE Zone='Battery Park') \
          ORDER BY tip_amount DESC LIMIT 1"

query2 = "SELECT * \
          FROM tripdata_table \
          WHERE tolls_amount IN (SELECT max(tolls_amount) \
                                 FROM tripdata_table \
                                 GROUP BY month(tpep_pickup_datetime) \
                                 ORDER BY month(tpep_pickup_datetime))"

query4 = "SELECT * \
          FROM (SELECT weekday, \
                       hour, \
                       passengers, \
                       row_number() OVER \
                          (PARTITION BY weekday ORDER BY passengers DESC) \
                        as hour_rank \
                FROM (SELECT DAYOFWEEK(tpep_pickup_datetime) as weekday, \
                             HOUR(tpep_pickup_datetime) as hour, \
                             avg(passenger_count) as passengers \
                      FROM tripdata_table \
                      GROUP BY weekday, hour)) \
          WHERE hour_rank<=3 \
          ORDER BY weekday, hour_rank"

query5 = "SELECT * \
          FROM  (SELECT day, \
                        tip_percentange, \
                        row_number() OVER \
                                      (PARTITION BY MONTH(day) ORDER BY tip_percentange DESC)\
                        as day_rank \
                 FROM (SELECT DATE(tpep_pickup_datetime) as day, \
                              avg(tip_amount/fare_amount * 100) as tip_percentange\
                       FROM tripdata_table \
                       GROUP BY day)) \
          WHERE day_rank<=5 \
          ORDER BY MONTH(day), day_rank"
              

#Query 1 SQL API 
total_time1 = 0
result1 = spark.sql(query1)
res1 = []
for i in range(10):
    start = time.time()
    res1 = result1.collect()
    total_time1 += time.time()-start #total time for query execution 

total_time1 = total_time1/10 #take the average time

# opening the csv file in 'w+' mode
file = open('/home/user/project/outputs/q1_result.csv', 'w+', newline ='')
 
# writing the data into the file
with file:   
    write = csv.writer(file)
    write.writerows(Row(res1[0].__fields__))
    write.writerows(res1)

#Query 2 SQL API
total_time2 = 0
result2 = spark.sql(query2)
res2 = []
for i in range(10):
    start = time.time()
    res2 = result2.collect()
    total_time2 += time.time()-start #total time for query execution

total_time2 = total_time2/10 #take the average time

# opening the csv file in 'w+' mode
file = open('/home/user/project/outputs/q2_result.csv', 'w+', newline ='')
 
# writing the data into the file
with file:   
    write = csv.writer(file)
    write.writerows(Row(res2[0].__fields__))
    write.writerows(res2)

#Query 3 === Dataframe API 
temp = tripdata.where(tripdata.PULocationID != tripdata.DOLocationID)
result3 = temp.groupBy(window("tpep_pickup_datetime", "15 days", startTime="2 days 22 hours"))\
.agg(avg("total_amount").alias("average amount"), avg("trip_distance").alias("average distance"))\
.sort("window").select('window.start', 'window.end', 'average amount', 'average distance')

total_time3 = 0
res3 = []
for i in range(10):
    start = time.time()
    res3 = result3.collect()
    total_time3 += time.time()-start #total time for query execution

total_time3 = total_time3/10 #take the average time

# opening the csv file in 'w+' mode
file = open('/home/user/project/outputs/q3_df_result.csv', 'w+', newline ='')
 
# writing the data into the file
with file:   
    write = csv.writer(file)
    write.writerows(Row(res3[0].__fields__))
    write.writerows(res3)

#Query 3 RDD API 
total_time3_rdd = 0
res3_rdd = []
for i in range(10):
    start = time.time()
    rdd1 = tripdata_rdd.filter(lambda x : x.PULocationID != x.DOLocationID)
    rdd2 = rdd1.map(lambda x : (((x.tpep_pickup_datetime.timetuple().tm_yday-1)//15-1), (x.total_amount, x.trip_distance)))
    rdd3 = rdd2.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    countsByKey = sc.broadcast(rdd2.countByKey())
    rdd4 = rdd3.map(lambda x : (x[0], x[1][0]/countsByKey.value[x[0]], x[1][1]/countsByKey.value[x[0]]))
    rdd5 = rdd4.sortBy(lambda x : x[0])
    res3_rdd = rdd5.collect()
    total_time3_rdd += time.time()-start 

total_time3_rdd = total_time3_rdd/10 

#write data to csv file locally
with open('/home/user/project/outputs/q3_rdd_result.csv', 'w+', newline='') as csvfile:
  csv_writer = csv.writer(csvfile)
  csv_writer.writerow(['fortnight of year', 'average amount', 'average distance'])
  for data in res3_rdd:
    row = list(data)
    csv_writer.writerow(row)

#Query 4 SQL API 
total_time4 = 0
result4 = spark.sql(query4)
res4 = []
for i in range(10):
    start = time.time()
    res4 = result4.collect()
    total_time4 += time.time()-start #total time for query execution

total_time4 = total_time4/10 #take the average time

# opening the csv file in 'w+' mode
file = open('/home/user/project/outputs/q4_result.csv', 'w+', newline ='')
 
# writing the data into the file
with file:   
    write = csv.writer(file)
    write.writerows(Row(res4[0].__fields__))
    write.writerows(res4)

#Query 5 SQL API 
total_time5 = 0
result5 = spark.sql(query5)
res5 = []
for i in range(10):
    start = time.time()
    res5 = result5.collect()
    total_time5 += time.time()-start #total time for query execution

total_time5 = total_time5/10 #take the average time

# opening the csv file in 'w+' mode
file = open('/home/user/project/outputs/q5_result.csv', 'w+', newline ='')
 
# writing the data into the file
with file:   
    write = csv.writer(file)
    write.writerows(Row(res5[0].__fields__))
    write.writerows(res5)

#Save elapsed time for each query in csv file
data1 = ['1', 'SQL', total_time1]
data2 = ['2', 'SQL', total_time2]
data3_df = ['3', 'DF', total_time3]
data3_rdd = ['3', 'RDD', total_time3_rdd]
data4 = ['4', 'SQL', total_time4]
data5 = ['5', 'SQL', total_time5]
with open('/home/user/project/time_outputs.csv', 'a', newline='') as csvfile:
  csv_writer = csv.writer(csvfile)
  csv_writer.writerow(['Query', 'Type', 'Time'])
  csv_writer.writerow(data1)
  csv_writer.writerow(data2)
  csv_writer.writerow(data3_df)
  csv_writer.writerow(data3_rdd)
  csv_writer.writerow(data4)
  csv_writer.writerow(data5)