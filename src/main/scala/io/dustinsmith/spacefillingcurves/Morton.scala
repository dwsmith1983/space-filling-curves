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

import Binary.getBinaryFunc
import com.typesafe.scalalogging.LazyLogging
import scalaz.Scalaz._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._


/**
 * Morton Ordering (Z ordering) is a method to map a multidimensional index down to
 * 1D. We do this be determine the z-index which we can sort by.
 *
 * @param df   Dataframe we wish to apply a Morton or Z index to.
 * @param cols The columns to order by in descending order or precedence.
 */
class Morton(val df: DataFrame, val cols: Array[String]) extends LazyLogging with SparkSessionWrapper {
  import spark.implicits._

  if (cols.length == 1) {
    throw new Exception("You need at least 2 columns to morton order your data.")
  }

  private val columnTypes: Seq[(String, String)] = matchColumnWithType()
  private val nonString = columnTypes.filter(t => t._2 != "StringType")
  private val stringType = columnTypes.filter(t => t._2 == "StringType")

  /**
   * UDF to interleave binary bits for the desired columns.
   */
  private def interleaveBits: UserDefinedFunction = udf {
    (colStruct: Row) =>

      // first item in struct is the number of z-ordering columns
      val numCols: Int = colStruct.getAs[Int](0)
      val dataArray: Array[String] = (1 to numCols).toArray.map(i => colStruct.getAs[String](i))
      val bits: Array[Int] = (0 until 64).toArray

      bits
        .map(c => dataArray.map(bin => bin(c).toString).mkString(""))
        .reduceLeft((x, y) => x + y)
  }

  /**
   * Matches the column name with the data type.
   *
   * @return A sequence of 2 tuples (col_name, col_data_type).
   */
  private def matchColumnWithType(): Seq[(String, String)] = {

    df.schema
      .map(
        structField =>
          if (cols.contains(structField.name)) (structField.name, structField.dataType.toString)
          else null
      )
      .filter(_ != null)
  }

  /**
   * Creates the z-index column in the dataframe.
   *
   * @return Returns the original dataframe with the additional z-index column.
   */
  def mortonIndex(): DataFrame = {

    val toIndex: DataFrame = getBinaryDF
      .withColumn("size", lit(cols.length))
      .withColumn("struct_bits",
        struct(Seq(col("size")) ++ cols.map(c => col(c + "_binary")): _*))
      .drop(Seq("size") ++ cols.map(c => c + "_binary"): _*)

    toIndex.withColumn("z_index", interleaveBits($"struct_bits")).drop("struct_bits")
  }

  /**
   * Appends a binary column proxy value for string columns with the
   * previously determined binary value for numerical columns.
   *
   * @return Dataframe with binary values for the columns to z-index.
   */
  private def getBinaryDF: DataFrame = {

    val dfNonStringBinary: DataFrame = getNonStringBinaryDF

    if (stringType.nonEmpty) {
      // mapping unique string to integer; not a fan of this approach
      // TODO: find another approach to assign strings to a proxy integer in order
      //  to easily convert to binary
      val stringMappings = stringType.map(
        t =>
          (
            t._1,
            "IntegerType",
            df
              .select(t._1)
              .distinct
              .withColumn("rn", row_number().over(Window.orderBy(monotonically_increasing_id())))
              .collect
              .map(r => Map(r(0).toString -> r(1).toString.toInt))
              .reduceLeft(_ |+| _)
          )
      )
        .map(t => (t._1, t._2, udf((c: String) => t._3(c))))

      dfNonStringBinary
        .select($"*" +: stringMappings.map(tup => tup._3(col(tup._1)).alias(tup._1 + "_bits")): _*)
        .select($"*" +: stringMappings.map(tup => getBinaryFunc(tup._2)(col(tup._1 + "_bits"))
          .alias(tup._1 + "_binary")): _*)
        .drop(stringMappings.map(t => t._1 + "_bits"): _*)
    }
    else dfNonStringBinary
  }

  /**
   * Return the dataframe with binary columns for the numerical datatypes.
   *
   * @return Dataframe binary values for the numerical columns to z-index.
   */
  private def getNonStringBinaryDF: DataFrame = {

    if (nonString.nonEmpty) {
      df
        .select($"*" +: nonString.map(tup => getBinaryFunc(tup._2)(col(tup._1)).alias(tup._1 + "_binary")): _*)
    }
    else df
  }
}
