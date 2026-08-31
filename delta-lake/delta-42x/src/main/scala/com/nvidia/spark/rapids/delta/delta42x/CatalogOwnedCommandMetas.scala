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

package com.nvidia.spark.rapids.delta.delta42x

import scala.reflect.classTag

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta,
  RunnableCommandRule}

import org.apache.spark.sql.delta.commands.{DeltaCommand, DeltaReorgTableCommand,
  OptimizeTableCommand}

class OptimizeTableCommandMeta(
    cmd: OptimizeTableCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends com.nvidia.spark.rapids.delta.common.OptimizeTableCommandMeta(
    cmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    super.tagSelfForGpu()
    if (getDeltaLogForOptimize().unsafeVolatileSnapshot.isCatalogOwned) {
      willNotWorkOnGpu("Delta 4.2 requires catalog-managed OPTIMIZE to run on CPU")
    }
  }
}

object DeltaReorgTableCommandMeta {
  private val optimizeCommandConfKey = "spark.rapids.sql.command.OptimizeTableCommand"

  def rule: RunnableCommandRule[DeltaReorgTableCommand] = {
    new RunnableCommandRule[DeltaReorgTableCommand](
      (cmd, conf, parent, rule) =>
        new DeltaReorgTableCommandMeta(cmd, conf, parent, rule),
      "Reorganize a Delta Lake table",
      classTag[DeltaReorgTableCommand]) {
      override def confKey: String = optimizeCommandConfKey
    }
  }
}

class DeltaReorgTableCommandMeta(
    cmd: DeltaReorgTableCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends com.nvidia.spark.rapids.delta.common.DeltaReorgTableCommandMeta(
    cmd, conf, parent, rule) {

  private object DeltaCmdProxy extends DeltaCommand

  override def tagSelfForGpu(): Unit = {
    super.tagSelfForGpu()
    val snapshot = DeltaCmdProxy.getDeltaTable(cmd.target, "REORG").deltaLog.unsafeVolatileSnapshot
    if (snapshot.isCatalogOwned) {
      willNotWorkOnGpu("Delta 4.2 requires catalog-managed REORG to run on CPU")
    }
  }
}
