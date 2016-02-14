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

package org.mentha.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.reflect.ClassTag

/** */
object DStreamHelpers {

  @transient
  private val stackedStreams: mutable.WeakHashMap[DStream[_], String] = mutable.WeakHashMap.empty

  implicit class DStreamHelper[T: ClassTag](self: DStream[T]) extends Logging {

    @inline
    def stack(size: Int): DStream[T] = /*self.ssc.withScope*/ {

      if (size <= 0) {
        throw new Exception("Size must be positive")
      }

      // register previous stream with original slideDuration (with no action)
      // it preserves original batch/slide processing
      stackedStreams.synchronized {
        stackedStreams.getOrElseUpdate(self, {
          self.foreachRDD {
            (rdd, timestamp) => {
              // just show debug info, if it's enabled
              logDebug(s"stack: postfix = ${toPostfix(timestamp)}, partitions = ${rdd.getNumPartitions}, count = ${rdd.count()}")
            }
          }
          Thread.currentThread().getStackTrace.mkString("\n")
        })
      }

      // increase slideDuration (for next streams)
      val duration = self.slideDuration * size
      self.window(windowDuration = duration, slideDuration = duration)
    }

    @inline
    def toSinglePartition(): DStream[T] = /* self.ssc.withScope */ {
      self.transform(
        rdd => {
          rdd.getNumPartitions match {
            case 1 => rdd
            case _ => rdd.coalesce(numPartitions = 1, shuffle = false)
          }
        }
      )
    }

    @inline
    def storeRDD(toSinglePartition: Boolean)(foreachFunc: (RDD[T], String) => Unit): Unit = /* self.ssc.withScope */ {
      val stream: DStream[T] = toSinglePartition match {
        case true => self.toSinglePartition()
        case false => self
      }
      stream
        .foreachRDD {
          (rdd, timestamp) => {
            if (!rdd.isEmpty()) {
              val postfix = toPostfix(timestamp)
              logRDD("storeRDD", rdd, postfix)
              foreachFunc(rdd, postfix)
            }
          }
        }
    }

    @inline
    private def logRDD(prefix: String, rdd: RDD[T], postfix: String): Unit = {
      if (log.isDebugEnabled) {
        // use long output, if debug is enabled
        logDebug(s"${prefix}: postfix = ${postfix}, partitions = ${rdd.getNumPartitions}, count = ${rdd.count()}")
      } else {
        logInfo(s"${prefix}: postfix = ${postfix}")
      }
    }

    @inline
    private def toPostfix(timestamp: Time): String = {
      val postfix: String = new SimpleDateFormat("yyyy-MM-dd/HH-mm-ss.S").format(new Date(timestamp.milliseconds))
      postfix
    }
  }

}
