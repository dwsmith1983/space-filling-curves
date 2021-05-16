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
    (1, 1, 12.23, "a"),
    (4, 9, 5.05, "b"),
    (3, 0, 1.23, "c"),
    (2, 2, 100.4, "d"),
    (1, 25, 3.25, "a")
  ).toDF("x", "y", "amnt", "id")
  val mortonNum: Morton = new Morton(df, Array("x", "y"))
  val mortonMixed: Morton = new Morton(df, Array("x", "id", "amnt"))

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
}
