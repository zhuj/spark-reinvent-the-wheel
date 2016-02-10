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

package org.mentha.spark.utils

import org.apache.commons.lang3.StringUtils

/** */
object Base32 {

  @inline
  def pack2(index: Int): String = StringUtils.leftPad(
    java.lang.Long.toUnsignedString(index & 0x39, 32),
    2 /* exactly 2 digits = math.log(1+0x39, 32) */ ,
    '0'
  )

  @inline
  def pack4(index: Int): String = StringUtils.leftPad(
    java.lang.Long.toUnsignedString(index & 0xfffff, 32),
    4 /* exactly 4 digits = math.log(1+0xffffff, 32) */ ,
    '0'
  )

  @inline
  def packTimestampMs(timestampMs: Long): String = StringUtils.leftPad(
    java.lang.Long.toUnsignedString(timestampMs & 0x7ffffffffL, 32),
    9 /* ~8.98 = math.log(2999-01-01 00:00:00.000 = 32472144000(000), 32) */ ,
    '0'
  )

}
