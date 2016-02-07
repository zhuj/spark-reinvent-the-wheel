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

package org.mentha.spark.mapreduce

import java.net.URLEncoder

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.mentha.spark.utils.Base32

import scala.collection.concurrent.TrieMap

/**
  * {{{
  *   objectStream
  *      .map { obj => (getKey(obj), toJson(obj)) }
  *      .groupByKey()
  *      .foreachRDD {
  *        (rdd, time) => {
  *          if (rdd.count() > 0) {
  *            rdd
  *              .map { case (key, iter) => ((time.milliseconds, Seq(key)), iter) }
  *              .saveAsKeyBasedCollection(path = "s3a://...")
  *          }
  *        }
  *      }
  * }}}
  */
object KeyBasedCollectionRDD {

  implicit class KeyBasedCollectionRDD[V <: AnyRef](self: RDD[(KeyBasedCollectionOutputFormat.TSKeyPath, Iterable[V])]) extends Logging {

    def saveAsTextFile(path: String): Unit = {
      self
        .saveAsNewAPIHadoopFile(
          path = path,
          keyClass = classOf[KeyBasedCollectionOutputFormat.TSKeyPath],
          valueClass = classOf[Iterable[V]],
          outputFormatClass = classOf[KeyBasedCollectionOutputFormat.OutputFormat[V]]
        )
    }

  }

}

/** */
object KeyBasedCollectionOutputFormat {

  /** KeyChain with TimeStamp */
  type TSKeyPath = (Long, Seq[String])

  /** */
  class OutputFormat[V <: AnyRef] extends TextOutputFormat[TSKeyPath, Iterable[V]] {

    protected def constructKeyBasedPath(job: TaskAttemptContext, fullKey: TSKeyPath): Path = {
      val (rddTs, keys) = fullKey

      val encodedKey: String = keys
        .map {
          k => Option(StringUtils.trimToNull(String.valueOf(k)))
            .map { s => URLEncoder.encode(s, "UTF-8") }
            .getOrElse("---")
        }
        .mkString(Path.SEPARATOR)

      val taskId: TaskID = job.getTaskAttemptID.getTaskID

      val childPath: String = StringBuilder
        .newBuilder
        .append(encodedKey)
        .append(Path.SEPARATOR)
        .append(FileOutputFormat.getOutputName(job))
        .append('-')
        .append(Base32.packTimestampMs(rddTs))
        .append('-')
        .append(TaskID.getRepresentingCharacter(taskId.getTaskType))
        .append('-')
        .append(Base32.pack4(taskId.getJobID.getId))
        .append(Base32.pack2(taskId.getId))
        .result()

      val outputPath = FileOutputFormat.getOutputPath(job)
      new Path(outputPath, childPath)
    }

    protected def constructRecordWriter(job: TaskAttemptContext, outputPath: Path): RecordWriter[String, V] = {
      val conf: Configuration = job.getConfiguration
      val fs: FileSystem = outputPath.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(outputPath, false)
      val keyValueSeparator: String = conf.get(TextOutputFormat.SEPERATOR, "\t")
      new TextOutputFormat.LineRecordWriter[String, V](fileOut, keyValueSeparator)
    }

    protected def getRowKeyPrefix(fullKey: TSKeyPath, value: V): String = {
      null
    }

    override def getRecordWriter(job: TaskAttemptContext): RecordWriter[TSKeyPath, Iterable[V]] = {
      new RecordWriter[TSKeyPath, Iterable[V]]() {

        val writers = TrieMap.empty[Path, RecordWriter[String, V]]

        override def write(fullKey: TSKeyPath, values: Iterable[V]): Unit = {
          val keyBasedPath: Path = constructKeyBasedPath(job, fullKey)
          val writer: RecordWriter[String, V] = writers.getOrElseUpdate(keyBasedPath, constructRecordWriter(job, keyBasedPath))
          for (v <- values) {
            val key: String = getRowKeyPrefix(fullKey, v)
            writer.write(key, v)
          }
        }

        override def close(context: TaskAttemptContext): Unit = {
          for (v <- writers.values) {
            v.close(context)
          }
          writers.clear()
        }
      }
    }

    private lazy val committer = new DirectCommitter()
    override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = committer
  }

  /** */
  class DirectCommitter extends OutputCommitter {
    override def abortTask(taskContext: TaskAttemptContext): Unit = {}
    override def commitTask(taskContext: TaskAttemptContext): Unit = {}
    override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = true
    override def setupJob(jobContext: JobContext): Unit = {}
    override def setupTask(taskContext: TaskAttemptContext): Unit = {}
  }

}