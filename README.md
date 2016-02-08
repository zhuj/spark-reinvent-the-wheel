# spark-reinvent-the-wheel

[Apache Spark](http://spark.apache.org/docs/1.5.2/) related sandbox. 
It's really the collection of re-invented wheels.

Already invented Spark-related wheels:
* General DStream issues - [DStreamHelpers](src/main/scala/org/mentha/spark/streaming/DStreamHelpers.scala)
* Store data from PairDStream so that each key will be stored in a separate directory - [Code](src/main/scala/org/mentha/spark/mapreduce/KeyBasedCollection.scala), [Example](src/test/scala/org/mentha/spark/mapreduce/keyBasedCollectionRDD/Example.scala)  


Wheels with various shapes and sizes:
* Stringify integers with base=32 - [Base32](src/main/scala/org/mentha/spark/utils/Base32.scala)
