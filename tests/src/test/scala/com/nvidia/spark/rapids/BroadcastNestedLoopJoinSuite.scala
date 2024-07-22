/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuBroadcastNestedLoopJoinExec

class BroadcastNestedLoopJoinSuite extends SparkQueryCompareTestSuite {

  test("BroadcastNestedLoopJoinExec AQE off") {
    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set("spark.rapids.sql.batchSizeBytes", "20")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark).repartition(2)
//      val df2 = nonZeroLongsDf(spark).repartition(2)
      val df2 = longsDf(spark)
      val df3 = df1.crossJoin(broadcast(df2)).filter(df1.col("longs").gt(df2.col("longs")))
      val result = df3.collect()
      assert(result.length === 45)
      val plan = df3.queryExecution.executedPlan

      val nljCount =
        PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastNestedLoopJoinExec])
      assert(nljCount.size === 1)
    }, conf)
  }

  test("BroadcastNestedLoopJoinExec AQE on") {
    val conf = new SparkConf()
        .set("spark.rapids.sql.exec.BroadcastNestedLoopJoinExec", "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        // In some cases AQE can make the children not look like they are on the GPU
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ShuffleExchangeExec,RoundRobinPartitioning")
      .set("spark.rapids.sql.batchSizeBytes", "10")

    withGpuSparkSession(spark => {
//      val df1 = longsDf(spark).repartition(2)
//      val df2 = nonZeroLongsDf(spark).repartition(2)
      val df1 = longsDf(spark).repartition(2)
      //      val df2 = nonZeroLongsDf(spark).repartition(2)
      val df2 = longsDf(spark)
      val df3 = df1.crossJoin(broadcast(df2)).filter(df1.col("longs").gt(df2.col("longs")))
      val result = df3.collect()
      assert(result.length === 45)
      val plan = df3.queryExecution.executedPlan

      val nljCount =
        PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastNestedLoopJoinExec])

      assert(nljCount.size === 1)
    }, conf)
  }

}
