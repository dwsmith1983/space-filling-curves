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

import Binary.getBinaryFunc
import org.scalatest.BeforeAndAfterAll
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.reflect.io.Directory

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}


class BinarySpec extends AnyWordSpec
  with Matchers
  with PrivateMethodTester
  with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("BinaryMethodTesting")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    new Directory(new File("spark-warehouse")).deleteRecursively
    super.afterAll()
  }

  "toBinaryFormat" should {

    "convert a binary string into n-bits by adding leading zeros if necessary" in {
      val privateMethod: PrivateMethod[String] = PrivateMethod[String]('toBinaryFormat)
      val resultStringAdd: String = Binary invokePrivate privateMethod("10110", 8)
      val expectedStringAdd: String = "00010110"
      val resultString: String = Binary invokePrivate privateMethod("10010110", 8)
      val expectedString: String = "10010110"

      assert(resultStringAdd == expectedStringAdd)
      assert(resultString == expectedString)
    }
  }

  // TODO: Figure out how to unit test private UDFS
  //  need this capability for integer and double testing
  "intToBinary integer" ignore {

    "convert integer values to binary string" in {
      import spark.implicits._

      val df: DataFrame = Seq(1, 2, 3, 4).toDF("x")
        .withColumn("x", $"x".cast(IntegerType))
      val privateMethod: PrivateMethod[Column] = PrivateMethod[Column]('intToBinary)
      val resultArray: Array[(Int, String)] = df
        .withColumn("x_binary", Binary invokePrivate privateMethod($"x"))
        .sort("x")
        .collect
        .map(r => (r(0).toString.toInt, r(1).toString))
      val expectedArray: Array[(Int, String)] = Array(
        (1, "1"),
        (2, "10"),
        (3, "11"),
        (4, "100")
      )

      assert(resultArray sameElements expectedArray)
    }
  }

  "intToBinary long" ignore {

    "convert long values to binary string" in {
      import spark.implicits._

      val df: DataFrame = Seq(1, 2, 3, 4).toDF("x")
        .withColumn("x", $"x".cast(LongType))
      val privateMethod: PrivateMethod[Column] = PrivateMethod[Column]('intToBinary)
      val resultArray: Array[(Int, String)] = df
        .withColumn("x_binary", Binary invokePrivate privateMethod($"x"))
        .sort("x")
        .collect
        .map(r => (r(0).toString.toInt, r(1).toString))
      val expectedArray: Array[(Int, String)] = Array(
        (1, "1"),
        (2, "10"),
        (3, "11"),
        (4, "100")
      )

      assert(resultArray sameElements expectedArray)
    }
  }

  "intToBinary short" ignore {

    "convert short values to binary string" in {
      import spark.implicits._

      val df: DataFrame = Seq(1, 2, 3, 4).toDF("x")
        .withColumn("x", $"x".cast(ShortType))
      val privateMethod: PrivateMethod[Column] = PrivateMethod[Column]('intToBinary)
      val resultArray: Array[(Int, String)] = df
        .withColumn("x_binary", Binary invokePrivate privateMethod($"x"))
        .sort("x")
        .collect
        .map(r => (r(0).toString.toInt, r(1).toString))
      val expectedArray: Array[(Int, String)] = Array(
        (1, "1"),
        (2, "10"),
        (3, "11"),
        (4, "100")
      )

      assert(resultArray sameElements expectedArray)
    }
  }

  "getBinaryFunc error" should {

    "throw an exceptiong for wrong column types" in {
      val wrongType: String = "TimestampType"
      val thrown: Exception = the [Exception] thrownBy getBinaryFunc(wrongType)

      thrown.getMessage should equal(
        "Binary function must receive a column string name of Integer, Long, Double, or Float."
      )
    }
  }

  "getBinaryFunc integer, long, short" should {

    "return the UDF for long values" in {
      val intType: String = "IntegerType"
      val shortType: String = "ShortType"
      val longType: String = "LongType"

      val intFun: UserDefinedFunction = getBinaryFunc(intType)
      val shortFun: UserDefinedFunction = getBinaryFunc(shortType)
      val longFun: UserDefinedFunction = getBinaryFunc(longType)

      val expectedFun: UserDefinedFunction = getBinaryFunc("IntegerType")

      // TODO: Better way to show that the functions are correct?
      // only using IntegerType here since they should all return the same function
      assert(intFun.toString == expectedFun.toString)
      assert(shortFun.toString == expectedFun.toString)
      assert(longFun.toString == expectedFun.toString)
    }
  }

  "getBinaryFunc double, float, decimal" should {

    "return the UDF for double values" in {
      val doubleType: String = "DoubleType"
      val floatType: String = "FloatType"
      val decType: String = "DecimalType"

      val doubleFun: UserDefinedFunction = getBinaryFunc(doubleType)
      val floatFun: UserDefinedFunction = getBinaryFunc(floatType)
      val decFun: UserDefinedFunction = getBinaryFunc(decType)

      val expectedFun: UserDefinedFunction = getBinaryFunc("DoubleType")

      // TODO: Better way to show that the functions are correct?
      // only using DoubleType here since they should all return the same function
      assert(doubleFun.toString == expectedFun.toString)
      assert(floatFun.toString == expectedFun.toString)
      assert(decFun.toString == expectedFun.toString)
    }
  }
}
