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
package io.dustinsmith.bitinterleave

import io.dustinsmith.bitinterleave.Binary._
import io.dustinsmith.spark.SparkSessionWrapper
import scalaz.Scalaz._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class InterleaveBits(val df: DataFrame, val cols: Array[String])
    extends SparkSessionWrapper {

  import spark.implicits._

  if (cols.length == 1) {
    throw new Exception(
      "You need at least 2 columns to morton order your data."
    )
  }

  private val columnTypes: Seq[(String, String)] = matchColumnWithType()
  private val nonString = columnTypes.filter(t => t._2 != "StringType")
  private val stringType = columnTypes.filter(t => t._2 == "StringType")

  /** Matches the column name with the data type.
    *
    * @return A sequence of 2 tuples (col_name, col_data_type).
    */
  private def matchColumnWithType(): Seq[(String, String)] = {

    df.schema
      .map(structField =>
        if (cols.contains(structField.name))
          (structField.name, structField.dataType.toString)
        else null
      )
      .filter(_ != null)
  }

  /** Appends a binary column proxy value for string columns with the
    * previously determined binary value for numerical columns.
    *
    * @return Dataframe with binary values for the columns to z-index.
    */
  def getBinaryDF: DataFrame = {

    val dfNonStringBinary: DataFrame = getNonStringBinaryDF

    if (stringType.nonEmpty) {
      // mapping unique string to integer; not a fan of this approach
      // TODO: find another approach to assign strings to a proxy integer in order
      //  to easily convert to binary
      val stringMappings = stringType
        .map(t =>
          (
            t._1,
            "IntegerType",
            df
              .select(t._1)
              .distinct
              .withColumn(
                "rn",
                row_number().over(Window.orderBy(monotonically_increasing_id()))
              )
              .collect
              .map(r => Map(r(0).toString -> r(1).toString.toInt))
              .reduceLeft(_ |+| _)
          )
        )
        .map(t => (t._1, t._2, udf((c: String) => t._3(c))))

      dfNonStringBinary
        .select(
          $"*" +: stringMappings.map(tup =>
            tup._3(col(tup._1)).alias(tup._1 + "_bits")
          ): _*
        )
        .select(
          $"*" +: stringMappings.map(tup =>
            getBinaryFunc(tup._2)(col(tup._1 + "_bits"))
              .alias(tup._1 + "_binary")
          ): _*
        )
        .drop(stringMappings.map(t => t._1 + "_bits"): _*)
    } else dfNonStringBinary
  }

  /** Return the dataframe with binary columns for the numerical datatypes.
    *
    * @return Dataframe binary values for the numerical columns to z-index.
    */
  private def getNonStringBinaryDF: DataFrame = {

    if (nonString.nonEmpty) {
      df
        .select(
          $"*" +: nonString.map(tup =>
            getBinaryFunc(tup._2)(col(tup._1)).alias(tup._1 + "_binary")
          ): _*
        )
    } else df
  }
}
