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

package com.nvidia.spark.rapids

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BinaryExecNode, LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf

private case class PlanValidationLeafExec(marker: Int = 0) extends LeafExecNode {
  override def output: Seq[Attribute] = Seq.empty
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationLeafExec is not executable")
}

private case class PlanValidationUnaryExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationUnaryExec is not executable")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

private case class PlanValidationBinaryExec(left: SparkPlan, right: SparkPlan)
    extends BinaryExecNode {
  override def output: Seq[Attribute] = left.output ++ right.output
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationBinaryExec is not executable")
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)
}

class TestPlanValidatorSuite extends AnyFunSuite {
  private def context(
      marker: String,
      requiredExec: String = ""): TestPlanValidator.ValidationContext = {
    TestPlanValidator.ValidationContext(Map(
      RapidsConf.SQL_ENABLED.key -> "true",
      RapidsConf.TEST_CONF.key -> "true",
      RapidsConf.TEST_ALLOWED_NONGPU.key -> Seq(
        "PlanValidationLeafExec",
        "PlanValidationUnaryExec",
        "PlanValidationBinaryExec").mkString(","),
      RapidsConf.TEST_VALIDATE_EXECS_ONGPU.key -> requiredExec,
      "spark.rapids.sql.test.validation.marker" -> marker), adaptiveEnabled = false)
  }

  test("nearest context wins when a later planning pass wraps an older pass") {
    val olderContext = context("older", "PlanValidationLeafExec")
    val newerContext = context("newer", "MissingFromFinalPlan")
    val olderPlan = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), olderContext)
    val finalPlan = TestPlanValidator.tagForValidation(
      PlanValidationUnaryExec(olderPlan), newerContext)

    assert(TestPlanValidator.resolveValidationContext(finalPlan).contains(newerContext))
    val error = intercept[IllegalArgumentException] {
      TestPlanValidator.validatePlan(finalPlan)
    }
    assert(error.getMessage.contains("MissingFromFinalPlan"))
  }

  test("an untagged final wrapper resolves the nearest tagged descendant") {
    val expectedContext = context("wrapped")
    val taggedPlan = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), expectedContext)
    val finalPlan = PlanValidationUnaryExec(taggedPlan)

    assert(TestPlanValidator.resolveValidationContext(finalPlan).contains(expectedContext))
    TestPlanValidator.validatePlan(finalPlan)
  }

  test("different contexts at the same nearest depth are rejected") {
    val left = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), context("left"))
    val right = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), context("right"))
    val finalPlan = PlanValidationBinaryExec(left, right)

    val error = intercept[IllegalArgumentException] {
      TestPlanValidator.resolveValidationContext(finalPlan)
    }
    assert(error.getMessage.contains("ambiguous validation contexts"))
  }

  test("validation uses the tagged configuration instead of mutable SQLConf") {
    val taggedContext = context("snapshot", "MissingFromSnapshot")
    val finalPlan = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), taggedContext)
    val sqlConf = SQLConf.get
    val key = RapidsConf.TEST_VALIDATE_EXECS_ONGPU.key
    val previousValue = sqlConf.getConfString(key, null)
    sqlConf.setConfString(key, "PlanValidationLeafExec")
    try {
      val error = intercept[IllegalArgumentException] {
        TestPlanValidator.validatePlan(finalPlan)
      }
      assert(error.getMessage.contains("MissingFromSnapshot"))
    } finally {
      if (previousValue == null) {
        sqlConf.unsetConf(key)
      } else {
        sqlConf.setConfString(key, previousValue)
      }
    }
  }
}
