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

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuMergeIntoCommand}
import org.apache.spark.storage.StorageLevel

/**
 * Delta 4.2 adapter for the shared GPU merge command.
 *
 * Delta 4.2 moved the materialization state into abstract members on
 * `MergeIntoMaterializeSource`. Declaring them here keeps the Delta 4.2 command compatible when
 * the aggregate plugin JAR also contains the Delta 4.0/4.1 implementation of the shared class.
 */
class GpuMergeIntoCommand42x(mergeCmd: MergeIntoCommand, conf: RapidsConf)
  extends GpuMergeIntoCommand(
    mergeCmd.source,
    mergeCmd.target,
    mergeCmd.catalogTable,
    mergeCmd.targetFileIndex,
    new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
    mergeCmd.condition,
    mergeCmd.matchedClauses,
    mergeCmd.notMatchedClauses,
    mergeCmd.notMatchedBySourceClauses,
    mergeCmd.migratedSchema,
    mergeCmd.trackHighWaterMarks,
    mergeCmd.schemaEvolutionEnabled)(conf) {

  private var materializeSource: Boolean = _
  private var materializeSourceStorageLevel: StorageLevel = _

  // These identifiers must match Delta's compiler-qualified private trait accessors exactly.
  // scalastyle:off line.size.limit
  def org$apache$spark$sql$delta$commands$merge$MergeIntoMaterializeSource$$materializeSource: Boolean =
    materializeSource

  def org$apache$spark$sql$delta$commands$merge$MergeIntoMaterializeSource$$materializeSource_=(
      enabled: Boolean): Unit = {
    materializeSource = enabled
  }

  def org$apache$spark$sql$delta$commands$merge$MergeIntoMaterializeSource$$materializeSourceStorageLevel:
      StorageLevel = materializeSourceStorageLevel

  def org$apache$spark$sql$delta$commands$merge$MergeIntoMaterializeSource$$materializeSourceStorageLevel_=(
      storageLevel: StorageLevel): Unit = {
    materializeSourceStorageLevel = storageLevel
  }
  // scalastyle:on line.size.limit
}
