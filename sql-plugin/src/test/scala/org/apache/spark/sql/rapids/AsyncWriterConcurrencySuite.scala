package org.apache.spark.sql.rapids

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.{Duration, MILLISECONDS}

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.util.ThreadUtils

class AsyncWriterConcurrencySuite extends AnyFunSuite with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private var asyncWriter: AsyncWriter = _
  private val numThreads = 4
  private val ec: ExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("AsyncWriterConcurrencySuite", numThreads)
  )
  private var writerId: Int = _

  override def beforeAll(): Unit = {
    asyncWriter = new AsyncWriter(1)
  }

  override def afterAll(): Unit = {
    asyncWriter.close()
  }

  override def beforeEach(): Unit = {
    writerId = asyncWriter.register()
  }

  override def afterEach(): Unit = {
    asyncWriter.deregister(writerId)
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

  test("test multiple threads scheduling tasks") {
    val latch = new java.util.concurrent.CountDownLatch(numThreads)
    for (_ <- 0 until numThreads) {
      ec.execute(() => {
        scheduleTask(new SleepTask(writerId, 1000))
        latch.countDown()
      })
    }
    latch.await()
    val flushing = asyncWriter.flush(writerId)
    Await.ready(flushing, Duration(5000, MILLISECONDS))
  }
}
