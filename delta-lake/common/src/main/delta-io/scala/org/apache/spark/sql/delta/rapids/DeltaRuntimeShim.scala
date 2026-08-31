/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.{RapidsConf, ShimReflectionUtils}
import com.nvidia.spark.rapids.delta.{DeltaConfigChecker, DeltaProvider}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.util.Clock

case class StartTransactionArg(log: DeltaLog, conf: RapidsConf, clock: Clock,
    catalogTable: Option[CatalogTable] = None, snapshot: Option[Snapshot] = None)

trait DeltaRuntimeShim {
  def getDeltaConfigChecker: DeltaConfigChecker
  def getDeltaProvider: DeltaProvider
  def startTransaction(log: DeltaLog, conf: RapidsConf, clock: Clock)
  : GpuOptimisticTransactionBase = {
    startTransaction(StartTransactionArg(log, conf, clock))
  }
  def startTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase
  def stringFromStringUdf(f: String => String): UserDefinedFunction
  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot
  def fileFormatFromLog(deltaLog: DeltaLog): FileFormat

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): RunnableCommand = {
    GpuWriteIntoDelta(gpuDeltaLog, cpuWrite)
  }

  def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    throw new UnsupportedOperationException("Write operation metadata is not implemented")
  }

  def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    throw new UnsupportedOperationException("Replace table metadata is not implemented")
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog
}

object DeltaRuntimeShim {
  private val Delta33xVersions = Set("3.3.0", "3.3.1", "3.3.2")

  private val SparkVersion = """^(\d+)\.(\d+)\.(\d+).*""".r

  private def parseSparkVersion(sparkVersion: String): (Int, Int, Int) = sparkVersion match {
    case SparkVersion(major, minor, patch) => (major.toInt, minor.toInt, patch.toInt)
    case _ => throw new IllegalStateException(s"Unable to parse Spark version $sparkVersion")
  }

  private[rapids] def getShimClassName(deltaVersion: String, sparkVersion: String): String = {
    val parsedSparkVersion = parseSparkVersion(sparkVersion)
    val shimClassName = (deltaVersion, parsedSparkVersion) match {
      case (version, (3, 2, _)) if version.startsWith("2.0.") =>
        Some("org.apache.spark.sql.delta.rapids.delta20x.Delta20xRuntimeShim")
      case ("2.1.1", (3, 3, _)) =>
        Some("org.apache.spark.sql.delta.rapids.delta21x.Delta21xRuntimeShim")
      case ("2.2.0", (3, 3, _)) =>
        Some("org.apache.spark.sql.delta.rapids.delta22x.Delta22xRuntimeShim")
      case ("2.3.0", (3, 3, _)) =>
        Some("org.apache.spark.sql.delta.rapids.delta23x.Delta23xRuntimeShim")
      case ("2.4.0", (3, 4, _)) =>
        Some("org.apache.spark.sql.delta.rapids.delta24x.Delta24xRuntimeShim")
      case (version, (3, 5, patch)) if Delta33xVersions.contains(version) && patch >= 3 =>
        Some("org.apache.spark.sql.delta.rapids.delta33x.Delta33xRuntimeShim")
      case ("4.0.0", (4, 0, 0)) =>
        Some("org.apache.spark.sql.delta.rapids.delta40x.Delta40xRuntimeShim")
      case ("4.0.1", (4, 0, patch)) if patch >= 1 && patch <= 4 =>
        Some("org.apache.spark.sql.delta.rapids.delta40x.Delta40xRuntimeShim")
      case ("4.1.0", (4, 1, patch)) if patch <= 1 =>
        Some("org.apache.spark.sql.delta.rapids.delta41x.Delta41xRuntimeShim")
      case ("4.2.0", (4, 0, 1) | (4, 1, 1)) =>
        Some("org.apache.spark.sql.delta.rapids.delta42x.Delta42xRuntimeShim")
      case _ => None
    }
    shimClassName.getOrElse {
      throw new IllegalStateException(
        s"Unsupported Delta Lake $deltaVersion and Spark $sparkVersion combination")
    }
  }

  private lazy val shimInstance = {
    val shimClassName = getShimClassName(io.delta.VERSION, SPARK_VERSION)
    val shimClass = ShimReflectionUtils.loadClass(shimClassName)
    shimClass.getConstructor().newInstance().asInstanceOf[DeltaRuntimeShim]
  }

  def getDeltaProvider: DeltaProvider = shimInstance.getDeltaProvider

  def getDeltaConfigChecker: DeltaConfigChecker = {
    shimInstance.getDeltaConfigChecker
  }

  def startTransaction(txArg: StartTransactionArg): GpuOptimisticTransactionBase = {
    shimInstance.startTransaction(txArg)
  }

  def stringFromStringUdf(f: String => String): UserDefinedFunction =
    shimInstance.stringFromStringUdf(f)

  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot =
    shimInstance.unsafeVolatileSnapshotFromLog(deltaLog)

  def fileFormatFromLog(deltaLog: DeltaLog): FileFormat =
    shimInstance.fileFormatFromLog(deltaLog)

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): RunnableCommand = {
    shimInstance.createGpuWrite(gpuDeltaLog, cpuWrite)
  }

  def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    shimInstance.buildWriteOperation(mode, partitionColumns, options)
  }

  def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    shimInstance.buildReplaceTableOperation(
      metadata, isManaged, orCreate, asSelect, options, clusterBy, isV1SaveAsTableOverwrite)
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean =
    shimInstance.getTightBoundColumnOnFileInitDisabled(spark)

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog = {
    shimInstance.getGpuDeltaCatalog(cpuCatalog, rapidsConf)
  }
}
