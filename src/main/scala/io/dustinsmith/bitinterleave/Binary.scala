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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * Object for the binary functionality.
 */
object Binary {

  /**
   * Converts a binary string to 64bits.
   *
   * @param i      Binary string needing to be convert to 64bits.
   * @param digits Number of bits; default 64.
   * @return Binary string with leading zeros to convert to 64bits; used for interleaving.
   */
  def binaryFormat(i: String, digits: Int = 64): String = {

    String.format("%" + digits + "s", i).replace(' ', '0')
  }

  def toBinaryFormat: UserDefinedFunction = udf((c: String) =>
    String.format("%" + 64 + "s", c).replace(' ', '0')
  )

  /**
   * UDFs for converting Short, Int, Long and Float, Double, Decimal to binary string.
   */
  private def doubleToBinary: UserDefinedFunction = udf((c: Double) => binaryFormat(java.lang.Long.toBinaryString(
    java.lang.Double.doubleToRawLongBits(c)))
  )

  /**
   * Match the string name for the column type with the appropriate UDF.
   *
   * @param typeName String name for the column type to be converted to binary.
   * @return UDF to call on the dataframe.
   */
  def getBinaryFunc(typeName: String): UserDefinedFunction = typeName match {

    case typeName if Seq("IntegerType", "LongType", "ShortType").contains(typeName) => toBinaryFormat
    case typeName if Seq("FloatType", "DoubleType", "DecimalType").contains(typeName) => doubleToBinary
    case _ => throw new Exception(
      "Binary function must receive a column string name of Integer, Long, Double, or Float."
    )
  }
}
