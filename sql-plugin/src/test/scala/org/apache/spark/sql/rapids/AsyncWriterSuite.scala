/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import java.util.concurrent.ConcurrentLinkedDeque

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, MILLISECONDS}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AsyncWriterSuite extends AnyFunSuite with BeforeAndAfterAll {

  // TODO: close after all tests
  var asyncWriter: AsyncWriter = _

  override def beforeAll(): Unit = {
    asyncWriter = new AsyncWriter(2)
  }

  override def afterAll(): Unit = {
    asyncWriter.close()
  }

  class SleepTask(id: Int, durationMs: Long) extends Task(id) {

    override def run(): Unit = {
      System.err.println(s"Task $id sleeping for $durationMs ms")
      Thread.sleep(durationMs)
    }
  }

  def scheduleTask(task: Task): Unit = {
    val scheduled = asyncWriter.schedule(task)
    scheduled match {
      case Some(_) => // Do nothing
      case None => fail("Task not scheduled")
    }
  }

  test("test schedule a task and wait for it to finish") {
    val id = asyncWriter.register()
    try {
      val scheduled = asyncWriter.schedule(new SleepTask(id, 100))
      scheduled match {
        case Some(f) => Await.ready(f, Duration(1000, MILLISECONDS))
        case None => fail("Task not scheduled")
      }
    } finally {
      asyncWriter.deregister(id)
    }
  }

  test("test schedule multiple tasks for the same stream and wait for them to flush") {
    val id = asyncWriter.register()
    try {
      // Schedule 10 tasks
      for (_ <- 0 until 10) {
        scheduleTask(new SleepTask(id, 100))
      }
      val flushing = asyncWriter.flush(id)
      Await.ready(flushing, Duration(2000, MILLISECONDS))
    } finally {
      asyncWriter.deregister(id)
    }
  }

  test("test schedule multiple tasks for various streams and wait for one stream to flush") {
    val id1 = asyncWriter.register()
    val id2 = asyncWriter.register()
    try {
      // Schedule 5 tasks with id1
      for (_ <- 0 until 5) {
        scheduleTask(new SleepTask(id1, 100))
      }
      // Schedule 5 tasks with id2
      for (_ <- 0 until 5) {
        scheduleTask(new SleepTask(id2, 100))
      }

      val flushing = asyncWriter.flush(id1)
      Await.ready(flushing, Duration(600, MILLISECONDS))
    } finally {
      asyncWriter.deregister(id1)
      asyncWriter.deregister(id2)
    }
  }

  test("test schedule multiple tasks for various streams and cancel one of them") {
    val id1 = asyncWriter.register()
    val id2 = asyncWriter.register()
    try {
      // Schedule 5 tasks with id1 and id2, respectively
      for (_ <- 0 until 5) {
        scheduleTask(new SleepTask(id1, 100))
        scheduleTask(new SleepTask(id2, 100))
      }

      asyncWriter.cancelStream(id1)
      val flushing = asyncWriter.flush(id1)
      assert(flushing.isCompleted) // TODO: test if it has been cancelled
    } finally {
      asyncWriter.deregister(id1)
      asyncWriter.deregister(id2)
    }
  }

  test("test processing tasks in order") {
    class OrderedTask(id: Int, durationMs: Long, ordinal: Int, q: ConcurrentLinkedDeque[Int])
      extends Task(id) {
      override def run(): Unit = {
        Thread.sleep(durationMs)
        assert(q.offer(ordinal))
      }
    }

    val q1 = new ConcurrentLinkedDeque[Int]()
    val q2 = new ConcurrentLinkedDeque[Int]()

    val id1 = asyncWriter.register()
    val id2 = asyncWriter.register()
    try {
      for (i <- 0 until 10) {
        scheduleTask(new OrderedTask(id1, 100, i, q1))
        scheduleTask(new OrderedTask(id2, 100, i, q2))
      }
      val flushing1 = asyncWriter.flush(id1)
      val flushing2 = asyncWriter.flush(id2)
      Await.ready(flushing1, Duration(2000, MILLISECONDS))
      assert(q1.size() == 10)
      for (i <- 0 until 10) {
        assertResult(i)(q1.poll())
      }
      Await.ready(flushing2, Duration(200, MILLISECONDS))
      assert(q2.size() == 10)
      for (i <- 0 until 10) {
        assertResult(i)(q2.poll())
      }
    } finally {
      asyncWriter.deregister(id1)
      asyncWriter.deregister(id2)
    }
  }
}
