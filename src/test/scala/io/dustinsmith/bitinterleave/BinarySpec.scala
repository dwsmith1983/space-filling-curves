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

import io.dustinsmith.SparkSessionTestWrapper
import io.dustinsmith.bitinterleave.Binary.getBinaryFunc
import java.io.File
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.reflect.io.Directory

import org.apache.spark.sql.expressions.UserDefinedFunction

class BinarySpec
    extends AnyWordSpec
    with Matchers
    with PrivateMethodTester
    with BeforeAndAfterAll
    with SparkSessionTestWrapper {

  /* The proxy methods test the functionality of the UDFs in Binary. I cannot think of away to test
   * the UDFS in a dataframe.
   * If anyone knows how test like this should be done, let me know.
   */
  def proxyIntToBinary(i: Long): String = i.toBinaryString

  def proxyDoubleToBinary(i: Double): String =
    java.lang.Long.toBinaryString(java.lang.Double.doubleToRawLongBits(i))

  override def afterAll(): Unit = {
    new Directory(new File("spark-warehouse")).deleteRecursively
    super.afterAll()
  }

  "toBinaryFormat" should {

    "convert a binary string into n-bits by adding leading zeros if necessary" in {
      val privateMethod: PrivateMethod[String] =
        PrivateMethod[String]('toBinaryFormat)
      val resultStringAdd: String =
        Binary invokePrivate privateMethod("10110", 8)
      val expectedStringAdd: String = "00010110"
      val resultString: String =
        Binary invokePrivate privateMethod("10010110", 8)
      val expectedString: String = "10010110"

      assert(resultStringAdd == expectedStringAdd)
      assert(resultString == expectedString)
    }
  }

  // Using proxy methods to test the UDF column results
  "intToBinary integer" should {

    "convert integer values to binary string" in {
      val binaryRes: String = proxyIntToBinary(500)

      assert(binaryRes == "111110100")
    }
  }

  "intToBinary long" should {

    "convert long values to binary string" in {
      val binaryRes: String = proxyIntToBinary(200000000000L)

      assert(binaryRes == "10111010010000111011011101000000000000")
    }
  }

  "intToBinary short" should {

    "convert short values to binary string" in {
      val binaryRes: String = proxyIntToBinary(8.toShort)

      assert(binaryRes == "1000")
    }
  }

  "doubleToBinary double, float, dcial" should {

    "convert integer values to binary string" in {
      // in Spark, they should cast the same in the dataframe; will tested later
      val binaryRes: String = proxyDoubleToBinary(1.5)

      assert(
        binaryRes == "11111111111000000000000000000000000000000000000000000000000000"
      )
    }
  }

  "getBinaryFunc error" should {

    "throw an exceptiong for wrong column types" in {
      val wrongType: String = "TimestampType"
      val thrown: Exception = the[Exception] thrownBy getBinaryFunc(wrongType)

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
