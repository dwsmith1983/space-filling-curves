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

import io.dustinsmith.{HashDataFrame, SparkSessionTestWrapper}
import java.io.File
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.reflect.io.Directory

import org.apache.spark.sql.DataFrame

class InterleaveBitsSpec
    extends AnyWordSpec
    with Matchers
    with PrivateMethodTester
    with BeforeAndAfterAll
    with SparkSessionTestWrapper {

  import spark.implicits._

  val df: DataFrame = Seq(
    (1, 1, 12.23, "a", "m"),
    (4, 9, 5.05, "b", "m"),
    (3, 0, 1.23, "c", "f"),
    (2, 2, 100.4, "d", "f"),
    (1, 25, 3.25, "a", "m")
  ).toDF("x", "y", "amnt", "id", "sex")
  val interleaveNum: InterleaveBits = new InterleaveBits(df, Array("x", "y"))
  val interleaveStr: InterleaveBits = new InterleaveBits(df, Array("id", "sex"))
  val interleaveMixed: InterleaveBits =
    new InterleaveBits(df, Array("x", "id", "amnt"))

  override def afterAll(): Unit = {
    new Directory(new File("spark-warehouse")).deleteRecursively
    super.afterAll()
  }

  "matchColumnWithType numeric columns" should {

    "return a tuple with column and data type" in {
      val privateMethod: PrivateMethod[Seq[(String, String)]] =
        PrivateMethod[Seq[(String, String)]]('matchColumnWithType)
      val resultArray: Seq[(String, String)] =
        interleaveNum invokePrivate privateMethod()
      val expectedArray: Seq[(String, String)] =
        Seq(("x", "IntegerType"), ("y", "IntegerType"))

      assert(resultArray == expectedArray)
    }
  }

  "matchColumnWithType mixed columns" should {

    "return a tuple with column and data type" in {
      val privateMethod: PrivateMethod[Seq[(String, String)]] =
        PrivateMethod[Seq[(String, String)]]('matchColumnWithType)
      val resultArray: Seq[(String, String)] =
        interleaveMixed invokePrivate privateMethod()
      val expectedArray: Seq[(String, String)] =
        Seq(("x", "IntegerType"), ("amnt", "DoubleType"), ("id", "StringType"))

      assert(resultArray == expectedArray)
    }
  }

  "getNonStringBinaryDF str columns" should {

    "return the original dataframe" in {
      val privateMethod: PrivateMethod[DataFrame] =
        PrivateMethod[DataFrame]('getNonStringBinaryDF)
      val resultDF: DataFrame = interleaveStr invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(df, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }

  "getNonStringBinaryDF num columns" should {

    "return the original dataframe with the binary columns for numeric" in {
      val privateMethod: PrivateMethod[DataFrame] =
        PrivateMethod[DataFrame]('getNonStringBinaryDF)
      val resultDF: DataFrame = interleaveNum invokePrivate privateMethod()
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
      val privateMethod: PrivateMethod[DataFrame] =
        PrivateMethod[DataFrame]('getBinaryDF)
      val resultDF: DataFrame = interleaveStr invokePrivate privateMethod()
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
      val privateMethod: PrivateMethod[DataFrame] =
        PrivateMethod[DataFrame]('getBinaryDF)
      val resultDF: DataFrame = interleaveNum invokePrivate privateMethod()
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
      val privateMethod: PrivateMethod[DataFrame] =
        PrivateMethod[DataFrame]('getBinaryDF)
      val resultDF: DataFrame = interleaveMixed invokePrivate privateMethod()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = spark.read
        .format("parquet")
        .load(getClass.getResource("/mixed_binary").getPath)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }
}
