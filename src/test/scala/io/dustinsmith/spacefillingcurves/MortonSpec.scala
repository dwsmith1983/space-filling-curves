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

import java.io.File

import scala.reflect.io.Directory

import org.scalatest.BeforeAndAfterAll
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import org.apache.spark.sql.{DataFrame, SparkSession}


class MortonSpec extends AnyWordSpec with Matchers with PrivateMethodTester with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("MortonIndexTesting")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = {
    new Directory(new File("spark-warehouse")).deleteRecursively
    super.afterAll()
  }

  val df: DataFrame = Seq(
    (1, 1, 12.23, "a", "m"),
    (4, 9, 5.05, "b", "m"),
    (3, 0, 1.23, "c", "f"),
    (2, 2, 100.4, "d", "f"),
    (1, 25, 3.25, "a", "m")
  ).toDF("x", "y", "amnt", "id", "sex")
  val mortonNum: Morton = new Morton(df, Array("x", "y"))
  val mortonStr: Morton = new Morton(df, Array("id", "sex"))
  val mortonMixed: Morton = new Morton(df, Array("x", "id", "amnt"))

  /**
   * The proxy methods test the functionality of the UDFs in Binary. I cannot think of away to test
   * the UDFS in a dataframe.
   * If anyone knows how test like this should be done, let me know.
   */
  def proxyInterleaveBits(dataArray: Array[String]): String = {

    val bits: Array[Int] = (0 until 5).toArray

    bits
      .map(c => dataArray.map(bin => bin(c).toString).mkString(""))
      .reduceLeft((x, y) => x + y)}

  "matchColumnWithType numeric columns" should {

    "return a tuple with column and data type" in {
      val privateMethod: PrivateMethod[Seq[(String, String)]] =
        PrivateMethod[Seq[(String, String)]]('matchColumnWithType)
      val resultArray: Seq[(String, String)] = mortonNum invokePrivate privateMethod()
      val expectedArray: Seq[(String, String)] = Seq(("x", "IntegerType"), ("y", "IntegerType"))

      assert(resultArray == expectedArray)
    }
  }

  "matchColumnWithType mixed columns" should {

    "return a tuple with column and data type" in {
      val privateMethod: PrivateMethod[Seq[(String, String)]] =
        PrivateMethod[Seq[(String, String)]]('matchColumnWithType)
      val resultArray: Seq[(String, String)] = mortonMixed invokePrivate privateMethod()
      val expectedArray: Seq[(String, String)] = Seq(("x", "IntegerType"), ("amnt", "DoubleType"), ("id", "StringType"))

      assert(resultArray == expectedArray)
    }
  }

  "getNonStringBinaryDF str columns" should {

    "return the original dataframe" in {
      val privateMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getNonStringBinaryDF)
      val resultDF: DataFrame = mortonStr invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(df, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }

  "getNonStringBinaryDF num columns" should {

    "return the original dataframe with the binary columns for numeric" in {
      val privateMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getNonStringBinaryDF)
      val resultDF: DataFrame = mortonNum invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = spark.read
        .format("parquet")
        .load(getClass.getResource("/numeric_binary").getPath)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }

  "getBinaryDF str columns" should {

    "return the original dataframe with the binary columns for strings" in {
      val privateMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getBinaryDF)
      val resultDF: DataFrame = mortonStr invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = spark.read
        .format("parquet")
        .load(getClass.getResource("/str_binary").getPath)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }

  "getBinaryDF num columns" should {

    "return the original dataframe with the binary columns for numeric" in {
      val privateMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getBinaryDF)
      val resultDF: DataFrame = mortonNum invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = spark.read
        .format("parquet")
        .load(getClass.getResource("/numeric_binary").getPath)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }

  "getBinaryDF mixed columns" should {

    "return the original dataframe with the binary columns for numeric and string" in {
      val privateMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getBinaryDF)
      val resultDF: DataFrame = mortonMixed invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = spark.read
        .format("parquet")
        .load(getClass.getResource("/mixed_binary").getPath)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }

  // TODO: Figure out testing private UDFS
  "interleaveBits" should {

    "interleave the binary bit columns" in {
      val binaryArray: Array[String] = Array("11010", "01001")
      val resultInterleaved: String = proxyInterleaveBits(binaryArray)

      assert(resultInterleaved == "1011001001")
    }
  }

  "Morton class" should {

    "throw an exception ig supplied column array is not greater than 1" in {
      val thrown: Exception = the [Exception] thrownBy new Morton(df, Array("x"))

      thrown.getMessage should equal(
        "You need at least 2 columns to morton order your data."
      )
    }
  }

  "mortonIndex" should {

    "return a dataframe with the column z_index" in {
      val privateMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('mortonIndex)
      val resultDF: DataFrame = mortonNum invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = spark.read
        .format("parquet")
        .load(getClass.getResource("/z_index").getPath)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }
}
