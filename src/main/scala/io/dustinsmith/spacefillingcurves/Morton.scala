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

import io.dustinsmith.bitinterleave.InterleaveBits
import io.dustinsmith.spark.{SparkSessionWrapper, Udfs}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Morton Ordering (Z ordering) is a method to map a multidimensional index down to
  * 1D. We do this be determine the z-index which we can sort by.
  *
  * @param df   Dataframe we wish to apply a Morton or Z index to.
  * @param cols The columns to order by in descending order or precedence.
  */
class Morton(val df: DataFrame, val cols: Array[String])
    extends SparkSessionWrapper {

  import spark.implicits._

  private val interleaved: InterleaveBits = new InterleaveBits(df, cols)

  /** Creates the z-index column in the dataframe.
    *
    * @return Returns the original dataframe with the additional z-index column.
    */
  def mortonIndex(): DataFrame = {

    val toIndex: DataFrame = interleaved.getBinaryDF
      .withColumn("size", lit(cols.length))
      .withColumn(
        "array_bits",
        array(cols.map(c => col(c + "_binary")): _*)
      )
      .drop(Seq("size") ++ cols.map(c => c + "_binary"): _*)

    toIndex
      .withColumn("z_index", Udfs.interleaveBitsUdf($"array_bits"))
      .drop("array_bits")
  }
}
