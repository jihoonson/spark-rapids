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

package org.apache.spark.sql.rapids

import java.util.concurrent.TimeoutException

import org.mockito.Mockito.{doThrow, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}

class ShimmedExecutionPlanCaptureCallbackImplSuite
    extends AnyFunSuite with MockitoSugar {

  private def newListener(callbackImpl: ExecutionPlanCaptureCallbackBase) =
    new ExecutionPlanCaptureCallback {
      override private[rapids] def callback: ExecutionPlanCaptureCallbackBase = callbackImpl
    }

  private def verifyPlanCaptureFailureIsIgnored(planCaptureFailure: Throwable): Unit = {
    val callback = mock[ExecutionPlanCaptureCallbackBase]
    val queryExecution = mock[QueryExecution]
    doThrow(planCaptureFailure).when(callback)
      .captureForValidationIfNeeded("failedAction", queryExecution)

    newListener(callback).onFailure(
      "failedAction", queryExecution, new RuntimeException("primary query failure"))

    verify(callback).captureIfNeeded(queryExecution)
    verify(callback).captureForValidationIfNeeded("failedAction", queryExecution)
  }

  private class TestExecutionPlanCaptureCallback
      extends ShimmedExecutionPlanCaptureCallbackImpl {
    private var nextWaitFailure: Throwable = null

    def failNextWait(failure: Throwable): Unit = {
      nextWaitFailure = failure
    }

    override protected def waitUntilListenerBusEmpty(timeoutMillis: Long): Unit = {
      val failure = nextWaitFailure
      nextWaitFailure = null
      if (failure != null) {
        throw failure
      }
    }
  }

  test("listener-bus drain timeouts clear validation capture state") {
    val callback = new TestExecutionPlanCaptureCallback
    val queryExecution = mock[QueryExecution]
    when(queryExecution.executedPlan).thenReturn(mock[SparkPlan])

    val startTimeout = new TimeoutException("start timeout")
    callback.failNextWait(startTimeout)
    assert(intercept[TimeoutException] {
      callback.startValidation(1)
    } eq startTimeout)

    // A failed start must leave capture disabled.
    callback.captureForValidationIfNeeded("ignored-after-start-timeout", queryExecution)
    callback.startValidation(1)
    assert(callback.getValidationErrorWithTimeout(1) == null)

    callback.startValidation(1)
    callback.captureForValidationIfNeeded("cleared-after-finish-timeout", queryExecution)
    val finishTimeout = new TimeoutException("finish timeout")
    callback.failNextWait(finishTimeout)
    assert(intercept[TimeoutException] {
      callback.getValidationErrorWithTimeout(1)
    } eq finishTimeout)

    // A failed finish must disable capture and discard the captured plan.
    callback.captureForValidationIfNeeded("ignored-after-finish-timeout", queryExecution)
    callback.startValidation(1)
    assert(callback.getValidationErrorWithTimeout(1) == null)
  }

  test("failed query validation ignores NoClassDefFoundError while capturing the plan") {
    verifyPlanCaptureFailureIsIgnored(new NoClassDefFoundError("missing test class"))
  }

  test("failed query validation ignores NonFatal errors while capturing the plan") {
    verifyPlanCaptureFailureIsIgnored(new IllegalStateException("test plan unavailable"))
  }

  test("failed query validation propagates other fatal errors while capturing the plan") {
    val callback = mock[ExecutionPlanCaptureCallbackBase]
    val queryExecution = mock[QueryExecution]
    val fatalError = new LinkageError("fatal test error")
    doThrow(fatalError).when(callback)
      .captureForValidationIfNeeded("failedAction", queryExecution)

    val thrown = intercept[LinkageError] {
      newListener(callback).onFailure(
        "failedAction", queryExecution, new RuntimeException("primary query failure"))
    }

    assert(thrown eq fatalError)
  }
}
