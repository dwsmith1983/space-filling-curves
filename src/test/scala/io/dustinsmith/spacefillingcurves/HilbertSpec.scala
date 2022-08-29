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

import io.dustinsmith.{HashDataFrame, SparkSessionTestWrapper}
import java.io.File
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.reflect.io.Directory

import org.apache.spark.sql.DataFrame

class HilbertSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with SparkSessionTestWrapper {

  override def afterAll(): Unit = {
    new Directory(new File("spark-warehouse")).deleteRecursively
    super.afterAll()
  }

  import spark.implicits._
  val testArr: Array[Int] = (0 to 15 by 1).toArray[Int]
  val df: DataFrame = Seq(
    (0, 0),
    (0, 1),
    (0, 2),
    (0, 3),
    (1, 0),
    (1, 1),
    (1, 2),
    (1, 3),
    (3, 0),
    (3, 1),
    (3, 2),
    (3, 3),
    (2, 0),
    (2, 1),
    (2, 2),
    (2, 3)
  ).toDF("x", "y")

  def proxyGrayCode(arr: Array[Int]): Array[Int] = {

    arr.map(i => i ^ i >> 1)
  }

  "grayCode" should {

    "take an array of ints and return their new int order for each one" in {
      val resultArr: Array[Int] = proxyGrayCode(testArr)
      val expectedArr: Array[Int] =
        Array(0, 1, 3, 2, 6, 7, 5, 4, 12, 13, 15, 14, 10, 11, 9, 8)

      assert(resultArr sameElements expectedArr)
    }
  }

  "hilbertIndex" should {

    "convert a z-index to the hilbert-index" in {
      val resultDF: DataFrame =
        new Hilbert(df, Array("x", "y")).hilbertIndex().sort("hilbert_index")
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = Seq(
        (0, 0, 0),
        (0, 1, 1),
        (1, 1, 2),
        (1, 0, 3),
        (1, 3, 4),
        (1, 2, 5),
        (0, 2, 6),
        (0, 3, 7),
        (3, 3, 8),
        (3, 2, 9),
        (2, 2, 10),
        (2, 3, 11),
        (2, 0, 12),
        (2, 1, 13),
        (3, 1, 14),
        (3, 0, 15)
      ).toDF("x", "y", "hilbert_index")
        .sort("hilbert_index")
      val expextedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expextedChecksum)
    }
  }
}
