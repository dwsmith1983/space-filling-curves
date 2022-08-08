/*
 * Copyright 2021 DustinSmith.Io. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package io.dustinsmith.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Udfs {

  val interleaveBitsUdf: UserDefinedFunction = udf(interleaveBits _)

  /** UDF to interleave binary bits for the desired columns.
    */
  def interleaveBits(arrayOfBinaryStrings: Seq[String]): String = {
    // expects to get an Array of binary strings
    // 1 find longest string in a sequence
    val maLength = arrayOfBinaryStrings.maxBy(_.length).length
    // make all strings the same size
    // transpose and concat
    arrayOfBinaryStrings
      .map(binaryString =>
        binaryString + "-" * (maLength - binaryString.length)
      )
      .transpose
      .flatten
      .mkString("")
      .replaceAll("-", "")
  }
}
