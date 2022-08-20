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
import org.apache.spark.sql.DataFrame

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import scala.reflect.io.Directory

class MortonSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with SparkSessionTestWrapper {

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

  "Morton class" should {

    "throw an exception ig supplied column array is not greater than 1" in {
      val thrown: Exception = the[Exception] thrownBy new Morton(df, Array("x"))

      thrown.getMessage should equal(
        "You need at least 2 columns to morton order your data."
      )
    }
  }

  "mortonIndex" should {

    "return a dataframe with the column z_index" in {
      val resultDF: DataFrame = new Morton(df, Array("x", "y")).mortonIndex()
      val resultChecksum: Int = HashDataFrame.checksumDataFrame(resultDF, 1)
      val expectedDF: DataFrame = spark.read
        .format("parquet")
        .load(getClass.getResource("/z_index").getPath)
      val expectedChecksum: Int = HashDataFrame.checksumDataFrame(expectedDF, 1)

      assert(resultChecksum == expectedChecksum)
    }
  }
}
