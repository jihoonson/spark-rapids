/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "401"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import com.nvidia.spark.rapids.delta.{DeltaProvider, NoDeltaProvider}

class DeltaRuntimeShimSuite extends SparkQueryCompareTestSuite {
  test("delta provider resolves from the installed Delta Lake version") {
    val provider = DeltaProvider()
    assert(provider ne NoDeltaProvider)
    val expectedProvider = io.delta.VERSION match {
      case "4.0.0" | "4.0.1" => "Delta40xProvider"
      case "4.1.0" => "Delta41xProvider"
      case "4.2.0" => "Delta42xProvider"
    }
    assert(provider.getClass.getSimpleName == s"$expectedProvider$$")
  }

  test("delta runtime shim selection covers supported combinations") {
    val supported = Seq(
      ("2.1.1", "3.3.4", "delta21x"),
      ("2.2.0", "3.3.4", "delta22x"),
      ("2.3.0", "3.3.4", "delta23x"),
      ("2.4.0", "3.4.4", "delta24x"),
      ("3.3.0", "3.5.3", "delta33x"),
      ("3.3.2", "3.5.9", "delta33x"),
      ("4.0.0", "4.0.0", "delta40x"),
      ("4.0.1", "4.0.1", "delta40x"),
      ("4.0.1", "4.0.4", "delta40x"),
      ("4.1.0", "4.1.1", "delta41x"),
      ("4.2.0", "4.0.1", "delta42x"),
      ("4.2.0", "4.1.1", "delta42x"))

    supported.foreach { case (deltaVersion, sparkVersion, expectedShim) =>
      assert(DeltaRuntimeShim.getShimClassName(deltaVersion, sparkVersion).contains(expectedShim))
    }
  }

  test("delta runtime shim selection rejects unsupported combinations") {
    val unsupported = Seq(
      ("3.3.0", "3.5.2"),
      ("4.0.0", "4.0.1"),
      ("4.0.1", "4.0.0"),
      ("4.0.1", "4.0.5"),
      ("4.0.0", "4.1.1"),
      ("4.1.0", "4.1.2"),
      ("4.2.0", "4.0.0"),
      ("4.2.0", "4.1.0"),
      ("4.2.0", "4.1.2"))

    unsupported.foreach { case (deltaVersion, sparkVersion) =>
      val error = intercept[IllegalStateException] {
        DeltaRuntimeShim.getShimClassName(deltaVersion, sparkVersion)
      }
      assert(error.getMessage.contains(deltaVersion))
      assert(error.getMessage.contains(sparkVersion))
    }
  }
}
