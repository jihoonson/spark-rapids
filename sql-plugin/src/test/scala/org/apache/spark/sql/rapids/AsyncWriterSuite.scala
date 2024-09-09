package org.apache.spark.sql.rapids

import java.util.concurrent.ConcurrentLinkedDeque

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, MILLISECONDS}

import org.scalatest.funsuite.AnyFunSuite

class AsyncWriterSuite extends AnyFunSuite {

  class SleepTask(id: Int, durationMs: Long) extends Task(id) {

    override def run(): Unit = {
      System.err.println(s"Task $id sleeping for $durationMs ms")
      Thread.sleep(durationMs)
    }
  }

  def scheduleTask(task: Task): Unit = {
    val scheduled = AsyncWriter.schedule(task)
    scheduled match {
      case Some(_) => // Do nothing
      case None => fail("Task not scheduled")
    }
  }

  test("test schedule a task and wait for it to finish") {
    val id = AsyncWriter.register()
    try {
      val scheduled = AsyncWriter.schedule(new SleepTask(id, 100))
      scheduled match {
        case Some(f) => Await.ready(f, Duration(1000, MILLISECONDS))
        case None => fail("Task not scheduled")
      }
    } finally {
      AsyncWriter.deregister(id)
    }
  }

  test("test schedule multiple tasks for the same stream and wait for them to flush") {
    val id = AsyncWriter.register()
    try {
      // Schedule 10 tasks
      for (_ <- 0 until 10) {
        scheduleTask(new SleepTask(id, 100))
      }
      val flushing = AsyncWriter.flush(id)
      Await.ready(flushing, Duration(2000, MILLISECONDS))
    } finally {
      AsyncWriter.deregister(id)
    }
  }

  test("test schedule multiple tasks for various streams and wait for one stream to flush") {
    val id1 = AsyncWriter.register()
    val id2 = AsyncWriter.register()
    try {
      // Schedule 5 tasks with id1
      for (_ <- 0 until 5) {
        scheduleTask(new SleepTask(id1, 100))
      }
      // Schedule 5 tasks with id2
      for (_ <- 0 until 5) {
        scheduleTask(new SleepTask(id2, 100))
      }

      val flushing = AsyncWriter.flush(id1)
      Await.ready(flushing, Duration(600, MILLISECONDS))
    } finally {
      AsyncWriter.deregister(id1)
      AsyncWriter.deregister(id2)
    }
  }

  test("test schedule multiple tasks for various streams and cancel one of them") {
    val id1 = AsyncWriter.register()
    val id2 = AsyncWriter.register()
    try {
      // Schedule 5 tasks with id1 and id2, respectively
      for (_ <- 0 until 5) {
        scheduleTask(new SleepTask(id1, 100))
        scheduleTask(new SleepTask(id2, 100))
      }

      AsyncWriter.cancelStream(id1)
      val flushing = AsyncWriter.flush(id1)
      assert(flushing.isCompleted) // TODO: test if it has been cancelled
    } finally {
      AsyncWriter.deregister(id1)
      AsyncWriter.deregister(id2)
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

    val id1 = AsyncWriter.register()
    val id2 = AsyncWriter.register()
    try {
      for (i <- 0 until 10) {
        scheduleTask(new OrderedTask(id1, 100, i, q1))
        scheduleTask(new OrderedTask(id2, 100, i, q2))
      }
      var flushing = AsyncWriter.flush(id1)
      Await.ready(flushing, Duration(2000, MILLISECONDS))
      assert(q1.size() == 10)
      for (i <- 0 until 10) {
        assertResult(i)(q1.poll())
      }
      flushing = AsyncWriter.flush(id2)
      Await.ready(flushing, Duration(200, MILLISECONDS))
      assert(q2.size() == 10)
      for (i <- 0 until 10) {
        assertResult(i)(q2.poll())
      }
    } finally {
      AsyncWriter.deregister(id1)
      AsyncWriter.deregister(id2)
    }
  }
}
