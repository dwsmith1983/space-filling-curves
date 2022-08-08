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

package io.dustinsmith.spark;

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UdfsSpec extends AnyWordSpec with Matchers {
  "interleaveBits" should {
    "interleave the binary bit columns 2" in {
      val binaryArray: Seq[String] = Array("1111", "00000")
      Udfs.interleaveBits(binaryArray) should equal("101010100")
    }
    "interleave the binary bit columns 3" in {
      val binaryArray: Seq[String] = Array("1010111", "11111011", "11")
      Udfs.interleaveBits(binaryArray) should equal("11101111011110111")
    }
    "interleave the binary bit columns 4" in {
      val binaryArray: Seq[String] =
        Array("1010111", "11111011", "10101", "10001")
      Udfs.interleaveBits(binaryArray) should equal("1111010011100100111110111")
    }
  }
}
