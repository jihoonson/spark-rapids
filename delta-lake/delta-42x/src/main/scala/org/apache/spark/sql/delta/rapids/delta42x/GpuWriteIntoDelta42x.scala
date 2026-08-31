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

package org.apache.spark.sql.delta.rapids.delta42x

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.commands.{WriteIntoDelta, WriteIntoDeltaLike}
import org.apache.spark.sql.delta.commands.DMLUtils.TaggedCommitData
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuWriteIntoDelta}
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/**
 * Delta 4.2 adapter for the shared GPU WriteIntoDelta implementation.
 *
 * Delta 4.2 added a nested result type to [[WriteIntoDeltaLike]], which adds a JVM trait accessor
 * that is absent from the Delta 4.0/4.1 copy of [[GpuWriteIntoDelta]] retained by the aggregate
 * JAR. Mixing in the Delta 4.2 trait here supplies that accessor while the existing GPU write
 * implementation continues to handle the Delta 4.1-compatible behavior.
 */
case class GpuWriteIntoDelta42x(
    gpuDeltaLog: GpuDeltaLog,
    cpuWrite: WriteIntoDelta)
  extends LeafRunnableCommand with WriteIntoDeltaLike {

  private def delegate: GpuWriteIntoDelta = GpuWriteIntoDelta(gpuDeltaLog, cpuWrite)

  override def run(sparkSession: SparkSession): Seq[Row] = delegate.run(sparkSession)

  override def withNewWriterConfiguration(
      updatedConfiguration: Map[String, String]): WriteIntoDeltaLike = {
    copy(cpuWrite = cpuWrite.copy(configuration = updatedConfiguration))
  }

  override val configuration: Map[String, String] = cpuWrite.configuration
  override val data: DataFrame = cpuWrite.data
  override val deltaLog: DeltaLog = gpuDeltaLog.deltaLog

  override def writeAndReturnCommitData(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      clusterBySpecOpt: Option[ClusterBySpec],
      isTableReplace: Boolean): TaggedCommitData[Action] = {
    delegate.writeAndReturnCommitData(txn, sparkSession, clusterBySpecOpt, isTableReplace)
  }
}
