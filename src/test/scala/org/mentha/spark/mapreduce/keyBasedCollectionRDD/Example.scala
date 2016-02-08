/*
 * Copyright (C) 2015-2016 the original author or authors.
 * See the LICENCE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mentha.spark.mapreduce.keyBasedCollectionRDD

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.mentha.spark.mapreduce.KeyBasedCollectionRDD._

/** */
object Example {

  val batchDuration: Duration = Seconds(5)

  private def toRecord(ts: Long, line: String): String = {
    line
  }

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[8]")
      .set("spark.cores.max", "8")
      .set("spark.executor.cores", "8")

    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val ssc: StreamingContext = new StreamingContext(sc, batchDuration)

    val stream: InputDStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat]("/tmp/files-to-scan")

    stream
      .map { case (ts, t) => (ts.get, t.toString) }
      .map { case (ts, s) => (ts, s.split(' ').toSeq) }
      .map { case (ts, s1 :: s2) => (Seq(s1), toRecord(ts, s2.mkString(" "))) }
      .groupByKey()
      .saveAsKeyBasedTextFiles(basePath = "/tmp/output-folder")

    ssc.start()

    Thread.sleep(batchDuration.milliseconds)

    ssc.stop(
      stopSparkContext = false,
      stopGracefully = true
    )

    ssc.awaitTermination()
    sc.stop()

  }
}
