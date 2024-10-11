package org.apache.spark.sql.rapids

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.util.ThreadUtils

class AsyncWriter2ConcurrencySuite extends AnyFunSuite with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private var asyncWriter: AsyncWriter2 = _
  private val numThreads = 4
  private val ec: ExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("AsyncWriterConcurrencySuite", numThreads)
  )

  override def beforeAll(): Unit = {
    asyncWriter = new AsyncWriter2(numThreads)
  }

  override def afterAll(): Unit = {
    asyncWriter.close()
  }

  class SleepTask(id: Int, durationMs: Long) extends Task(id, 0) {

    override def run(): Unit = {
      System.err.println(s"Task $id sleeping for $durationMs ms")
      Thread.sleep(durationMs)
    }
  }

  def scheduleTask(task: Task): Future[Unit] = {
    val scheduled = asyncWriter.schedule(task)
    scheduled match {
      case Some(f) => f
      case None => fail("Task not scheduled")
    }
  }

  test("test multiple threads scheduling tasks") {
    val futures = for (_ <- 0 until numThreads) yield {
      val id = asyncWriter.register()
      var f: Future[Unit] = null
      for (_ <- 0 until 100) {
        f = scheduleTask(new SleepTask(id, 10))
      }
      f
    }
    for (f <- futures) {
      Await.ready(f, Duration(5000, MILLISECONDS))
    }
    for (id <- 0 until numThreads) {
      asyncWriter.deregister(id)
    }
  }
}
