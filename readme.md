## Overview

1. Architecture: 
   1. Lambda 
   2. Spark parallelism 
   3. Configs
   4. HashPartitioning vs RangePartitioning
   5. Resources using
   6. Driver resources 
   7. Worker CPU Network RAM Disk
   8. Configuring CPU/RAM
   9. Driver configs
   10. Dynamic Res Allocation
   11. Spark.range
   12. Object ResourceManager
   13. KillExecutor(1)
2. Language: Scala vs Java vs Python, Garbage Collection, Maven vs SBT
3. Spark Batch
   1. Low level API: RDD
      1. Partitions
      2. Partitioner
      3. Iterator
      4. Spark DSL
      5. Dependencies: wide and narrow
      6. Persist vs CheckPoint
      7. Query Plan
      8. Physical operator
   2. High level API: DataFrame
      1. Catalyst Optimiser: Logical & Physical plans
      2. Raw vs InternalRaw
      3. Spark DSL
      4. Spark SQL
      5. Window functions
      6. Spark Scala UDF + Anti-patterns
      7. Creating DataFrame
      8. InternalRow
      9. Raw vs InternalRaw
   3. High level API: DataSet
   4. Spark Optimisation
      1. DPP
      2. AQE
      3. Join optimisation
   5. Spark Testing
   6. Spark ML
4. Spark Streaming
   1. Spark Streaming
   2. Spark Structure Streaming
   3. Kafka Spark streaming vs batch vs Streaming
   4. Spark streaming checkpointing
   5. Cassandra pipeline
   6. State full streaming processing
   7. Flink Streaming analytics Low API
   8. Flink Streaming analytics High API
5. Spark Loading Data 
   1. HDFS, S3, Local File System
   2. Data Formats
   3. Jdbc, Mongo, Green Plum
   4. Custom connectors Batch
   5. Custom connectors Streaming
6. Spark Cluster
   1. Resource Manager: EMR vs EKS
   2. Orchestration
   3. CI/CD
   4. 

Links:
https://towardsdatascience.com/strategies-of-spark-join-c0e7b4572bcf
