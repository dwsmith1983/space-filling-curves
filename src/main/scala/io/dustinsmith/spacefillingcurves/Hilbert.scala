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
package io.dustinsmith.spacefillingcurves

import io.dustinsmith.spark.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class Hilbert(val df: DataFrame, val cols: Array[String])
    extends SparkSessionWrapper {

  import spark.implicits._

  private val interleavedDF: DataFrame = new Morton(df, cols).mortonIndex()

  /** Gray codes an integer value in binary and returns its new integer.
    *
    * @return Integer after bit hacking
    */
  private def grayCode: UserDefinedFunction = udf { (colBinary: String) =>
    val colInt: Integer = Integer.parseInt(colBinary, 2)
    colInt ^ colInt >> 1
  }

  /** Determines the Hilbert index from the z-index of Morton ordering.
    *
    * @return Dataframe with new hilbert_index column.
    */
  def hilbertIndex(): DataFrame = {

    interleavedDF
      .withColumn("hilbert_index", grayCode($"z_index"))
      .drop("z_index")
  }
}
