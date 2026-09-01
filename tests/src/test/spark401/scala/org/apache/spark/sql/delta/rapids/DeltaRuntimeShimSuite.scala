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

import java.lang.reflect.Modifier

import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.delta.{DeltaProvider, NoDeltaProvider}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.commands.{WriteIntoDelta, WriteIntoDeltaLike}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf

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

  test("GPU write factory has no default implementation") {
    val method = classOf[DeltaRuntimeShim].getMethod(
      "createGpuWrite", classOf[GpuDeltaLog], classOf[WriteIntoDelta])
    assert(Modifier.isAbstract(method.getModifiers))
  }

  test("Delta 4.2 GPU writes use the runtime-specific GPU implementation") {
    assume(io.delta.VERSION == "4.2.0")
    val deltaLog = mock[DeltaLog]
    val cpuWrite = WriteIntoDelta(
      deltaLog,
      SaveMode.Append,
      new DeltaOptions(Map.empty[String, String], new SQLConf),
      partitionColumns = Nil,
      configuration = Map.empty,
      data = mock[Dataset[Row]])
    val gpuWrite = DeltaRuntimeShim.createGpuWrite(
      new GpuDeltaLog(deltaLog, new RapidsConf(Map.empty[String, String])), cpuWrite)

    assert(gpuWrite.getClass.getSimpleName == "GpuWriteIntoDelta42x")
    assert(gpuWrite.isInstanceOf[GpuWriteIntoDeltaBase])
    assert(gpuWrite.isInstanceOf[LeafRunnableCommand])
    assert(gpuWrite.isInstanceOf[ImplicitMetadataOperation])
    assert(gpuWrite.withNewWriterConfiguration(Map("key" -> "value"))
      .getClass.getSimpleName == "GpuWriteIntoDelta42x")

    val accessor = classOf[WriteIntoDeltaLike]
      .getMethod("ReplaceWhereExprsAndDataFilterPresenceInExprs")
    assert(accessor.invoke(gpuWrite) != null)
  }
}
