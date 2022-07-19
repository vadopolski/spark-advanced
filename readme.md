## Overview

6 days per 4 hors

- Spark Batch
- Spark Streaming

Homework AS IS
1. Scala Application to parse JSON
2. Working with HDFS: reading, writing, manage files
3. First Data Marts based on Spark
4. Building custom connector for Spark
5. Cover Spark application by tests
6. Spark Streaming application for reading data from Kafka, Transforming, Writing to cassandra 
7. Spark Streaming application for reading data from Kafka, Transforming, Writing to cassandra
8. Hive Application
9. Building monitoring for the project
   
Homework TO BE
0. Scala Application to parse JSON
1. First Data Marts based on Spark
2. Building custom connector for Spark
3. Cover Spark application by tests
4. Spark Streaming app for reading data from Kafka, State, Cassandra
5. Flink Streaming app for reading data from Kafka, State, Cassandra

HomeWork from current
1. Module RDD
   1. Find dependencies, partitions of parent RDD
   2. Find 



1. Module 1 - RDD
   1. Find dependencies, partitions
   2. Find popular time for orders
2. Module 2 - DataFrame + DataSet
   1. Find most popular boroughs for orders
   2. Find distance distribution for orders grouped by boroughs
   3. Get Homework from 
3. Module 3 - External and Connectors
   1. Write your own connectors
   2. Write UDF for joining
4. Module 4 - Spark optimisation
   1. Compare crimes counting: SortMerge Join, BroadCast, BlumFilter
5. Module 5 - Spark streaming
   1. Build Spark Structure Reading
   2. Build Spark Structure Using State
   3. Build Spark Structure Writing
6. Module 6 - Spark Cluster
   1. Build config with allocation
   2. Compare several workers





10. Architecture: 
    1. Promo of Homework1 and promo of all necessary info for that  
    2. Lambda
    3. Spark parallelism (after 10 p)
    4. Configs
    5. MapReduce -> HashPartitioning (group by, join) vs RangePartitioning (order by)
    6. Resources using: Yarn vs Kubernetes
    7. Driver CPU Network RAM Disk resources 
    8. Executer CPU Network RAM Disk resources
    9. Configuring CPU/RAM
    10. Driver configs
    11. Dynamic Res Allocation
    12. Spark.range
    13. Object ResourceManager
    14. KillExecutor(1)
11. Language: Scala vs Java vs Python, Garbage Collection, Maven vs SBT (Move )
    1. Checking Homework 2 and promo Homework 1
12. Spark Batch
    1. Promo of Batch Homework, knowledge for that | Checking architecture 
    2. Checking Homework 2 and promo Homework 1
    3. Low level API: RDD
       1. Partitions
       2. Partitioner
       3. Iterator
       4. Spark DSL
       5. Dependencies: wide and narrow
       6. Persist vs CheckPoint
       7. Query Plan
       8. Physical operator
    4. High level API: DataFrame
       1. Catalyst Optimiser: Logical & Physical plans
       2. Raw vs InternalRaw
       3. Spark DSL
       4. Spark SQL
       5. Window functions
       6. Spark Scala UDF + Anti-patterns
       7. Creating DataFrame
       8. InternalRow
       9. Raw vs InternalRaw
    5. High level API: DataSet
    6. Spark Optimisation
       1. DPP
       2. AQE
       3. Join optimisation
       4. Skew Joins
    7. Spark Testing
    8. Spark ML
13. Spark Streaming
    1. Spark Streaming
    2. Spark Structure Streaming
    3. Kafka Spark streaming vs batch vs Streaming
    4. Spark streaming checkpointing
    5. Cassandra pipeline
    6. State full streaming processing
    7. Flink Streaming analytics Low API
    8. Flink Streaming analytics High API
14. Spark Loading Data 
    1. HDFS, S3, Local File System
    2. Data Formats
    3. Jdbc, Mongo, Green Plum
    4. Custom connectors Batch
    5. Custom connectors Streaming
15. Spark Cluster
    1. Resource Manager: EMR vs EKS
    2. Orchestration
    3. CI/CD
    4. 

Links:
https://towardsdatascience.com/strategies-of-spark-join-c0e7b4572bcf
