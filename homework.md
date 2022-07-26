1. Module 0 - Scala
   1. Write function - def toJSON(Iterator[InternalRaw]): Iterator[String] (example find in Spark - DataSet, json method)
   2. Using libs for parsing JSON write code which find 
   3. Theory
      1. var and val, val (x, x), lazy val, [transient lazy val](http://fdahms.com/2015/10/14/scala-and-the-transient-lazy-val-pattern/)
      2. type and Type, (Nil, null, None, Null, Nothing, Unit)
      3. class, object (case), abstract class, trait  
      4. Generic, ClassTag
      5. Pattern matching and if the else construction
      6. Mutable and Immutable collection, Iterator
      7. Monads (Option, Either, Try, Future, ....)
      8. map, flatMap, for comprehension
      9. Implicits,
      10. Scala function, methods, lambda
      11. Scala sbt, assembly
      12. Encoder, Product
      13. Scala libs
2. Module 1 - RDD
    1. [Exercise find popular time for orders](src/main/scala/ch3batch/lowlevel/DemoRDD.scala)
    2. Theory RDD api:
       1. RDD creating api: from array, from file. from DS
       2. RDD base operations: map, flatMap, filter, reduceByKey, sort 
       3. Time parse libs
    3. [Exercise find dependencies](src/main/scala/ch3batch/lowlevel/FindDependencies.scala)
    4. [Exercise make join without shuffle](src/main/scala/ch3batch/lowlevel/Join.scala)
    5. Theory RDD under the hood:
       1. Iterator + mapPartitions()
       2. RDD creating path: compute() and getPartitions()
       3. Partitions
       4. Partitioner: Hash and Range
       5. Dependencies: wide and narrow
       6. Joins: inner, cogroup, join without shuffle
       7. Query Plan
3. Module 2 - DataFrame & DataSet, Spark DSL & Spark SQL 
    1. [Exercise Find most popular boroughs for orders](src/main/scala/ch3batch/highlevel/DemoDataFrame.scala)
    2. [Find distance distribution for orders grouped by boroughs](src/main/scala/ch3batch/highlevel/DemoDataSet.scala)
    3. Theory DataFrame, DataSet api:
       1. Creating DataFrame: memory, from file (HDFS, S3, FS) (Avro, Orc, Parquet)
       2. Spark DSL: Join broadcast, Grouped operations
       3. Spark SQL: Window functions, single partitions, 
       4. Scala UDF Problem solving
       5. Spark catalog, ....,
    4. Recreate code using plans [Reverse engineering](src/main/scala/ch3batch/highlevel/dataframe.scala)
    5. Theory 
       1. Catalyst Optimiser: Logical & Physical plans
       2. Codegen
       3. Persist vs Cache vs Checkpoint
       4. Creating DataFrame Path
       5. Raw vs InternalRaw
       6. InternalRow
       7. Raw vs InternalRaw
4. Module 4 - Spark optimisation
    1. [Compare speed, size RDD, DataFrame, DataSet](src/main/scala/ch3batch/highlevel/DataFrameVsRDD.scala)
    2. Compare crimes counting: SortMerge Join, BroadCast, BlumFilter
    3. Resolve problem with a skew join
    4. Build UDF for
5. Module 3 - External and Connectors
    1. Write your own connectors
    2. Write UDF for joining with cassandra
    3.

6. 7. Module 5 - Spark streaming
    1. Build Spark Structure Reading Kafka
    2. Build Spark Structure Using State
    3. Build Spark Structure Writing Cassandra
8. Module 6 - Spark Cluster
    1. Build config with allocation
    2. Compare several workers






Labs Spark Advanced (find place in Spark)
1. Write two class which convert JSON String to InternalRaw and InternalRaw to JSON String (theory about UDF how to write Internal RAW + DataSet method to_json)
2. Write static data source
3. Add predicate push down to data source API
4. Add structure streaming API support to data source API

Modified homework:
1. Write function - def toJSON(Iterator[InternalRaw]): Iterator[String] (example find in Spark - DataSet, json method)
2. 