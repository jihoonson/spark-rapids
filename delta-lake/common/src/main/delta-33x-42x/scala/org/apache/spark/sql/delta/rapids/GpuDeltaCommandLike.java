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

package org.apache.spark.sql.delta.rapids;

import scala.Option;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.commands.DeltaCommand;

/**
 * Stable DeltaCommand parent for GPU Delta commands.
 *
 * Delta 4.2 adds {@code createTableRelation} to {@code DeltaCommand}. Defining that method on a
 * Java interface keeps the inherited method set stable when shared Scala commands are compiled
 * against Delta 4.0, 4.1, and 4.2. Java permits the same declaration whether or not the parent
 * interface already declares it, unlike Scala's version-dependent {@code override} requirement.
 */
public interface GpuDeltaCommandLike extends DeltaCommand {
  default LogicalPlan createTableRelation(
      OptimisticTransaction txn, Option<String> tableAliasOpt) {
    return GpuDeltaCommandUtils$.MODULE$.createTableRelation(txn, tableAliasOpt);
  }
}
