## Overview

1. Architecture: 
   1. Lambda 
   2. Spark parallelism 
   3. Configs
   4. HashPartitioning vs RangePartitioning
2. Language: Scala vs Java vs Python, Garbage Collection, Maven vs SBT
3. Spark Batch
   1. Low level API: RDD
      1. Partitions
      2. Iterator
      3. Spark DSL
      4. Dependencies: wide and narrow
      5. Persist vs CheckPoint
      6. Query Plan
      7. Physical operator
   2. High level API: DataFrame
      1. Catalyst Optimiser: Logical & Physical plans
      2. Spark DSL
      3. Spark SQL
      4. Window functions
      5. Spark UDF
      6. 
   3. High level API: DataSet
   4. Spark Optimisation 
   5. Spark Testing
   6. Spark ML
4. Spark Streaming
   1. Spark Streaming
   2. Spark Structure Streaming
   3. Kafka Spark Cassandra pipeline
   4. Flink Streaming analitics Low API
   5. Flink Streaming analitics High API
5. Spark Loading Data 
   1. HDFS, S3, Local File System
   2. Data Formats
   3. Jdbc, Mongo, Green Plum
   4. Custom connectors
6. Spark Cluster
   1. Resource Manager: EMR vs EKS
   2. Orchestration
   3. CI/CD
   4. 

