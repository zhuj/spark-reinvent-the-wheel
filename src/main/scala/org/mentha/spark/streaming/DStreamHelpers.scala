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

import java.util.Date

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/** */
object DStreamHelpers {

  implicit class DStreamHelper[T: ClassTag](self: DStream[T]) extends Logging {

    @inline
    def toSinglePartition(): DStream[T] = /* self.ssc.withScope */ {
      self.transform(
        rdd => {
          rdd.partitions.length match {
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
          (rdd, time) => {
            if (!rdd.isEmpty()) {
              val postfix: String = new java.text.SimpleDateFormat("yyyy-MM-dd/HH-mm-ss.S").format(new Date(time.milliseconds))
              logInfo(s"postfix = ${postfix}")
              foreachFunc(rdd, postfix)
            }
          }
        }
    }

  }

}