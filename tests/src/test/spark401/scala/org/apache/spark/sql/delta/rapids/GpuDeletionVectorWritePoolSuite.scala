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

/*** spark-rapids-shim-json-lines
{"spark": "401"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{Callable, CancellationException, CountDownLatch,
  ExecutionException, Executors, ExecutorService, Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.TaskContext

class GpuDeletionVectorWritePoolSuite extends AnyFunSuite with Eventually {
  private def await(latch: CountDownLatch): Unit = {
    assert(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for writer coordination")
  }

  private def result[T](future: Future[T]): T = future.get(10, TimeUnit.SECONDS)

  private def withProducers(body: ExecutorService => Unit): Unit = {
    val producers = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("dv-test-producer-%d").build())
    try {
      body(producers)
    } finally {
      producers.shutdownNow()
      assert(producers.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  private def async[T](producers: ExecutorService)(body: => T): Future[T] = {
    producers.submit(new Callable[T] {
      override def call(): T = body
    })
  }

  test("executor admission bounds both active writes and producer read-ahead") {
    withResource(new GpuDeletionVectorWritePool(2)) { pool =>
      withProducers { submit =>
        val active = new AtomicInteger
        val peak = new AtomicInteger
        val reads = new AtomicInteger
        val running = new CountDownLatch(2)
        val windowFull = new CountDownLatch(4)
        val release = new CountDownLatch(1)
        val bins = (0 until 20).iterator.map { bin =>
          reads.incrementAndGet()
          windowFull.countDown()
          bin
        }
        val iterator = new GpuDeletionVectorWriteIterator(bins, pool, (bin: Int) => {
          val count = active.incrementAndGet()
          peak.accumulateAndGet(count, (a: Int, b: Int) => math.max(a, b))
          running.countDown()
          try {
            await(release)
            Seq(bin)
          } finally {
            active.decrementAndGet()
          }
        })
        val collected = async(submit)(iterator.toList)
        try {
          await(running)
          await(windowFull)
          assert(reads.get == 4)
          assert(peak.get == 2)
        } finally {
          release.countDown()
        }
        assert(result(collected).sorted == (0 until 20).toList)
        assert(peak.get == 2)
        assert(active.get == 0)
      }
    }
  }

  test("completed bins are returned before an earlier slow bin closes") {
    withResource(new GpuDeletionVectorWritePool(2)) { pool =>
      withProducers { submit =>
        val slowStarted = new CountDownLatch(1)
        val releaseSlow = new CountDownLatch(1)
        val iterator = new GpuDeletionVectorWriteIterator(Iterator(0, 1), pool,
          (bin: Int) => {
            if (bin == 0) {
              slowStarted.countDown()
              await(releaseSlow)
            }
            Seq(bin)
          })
        val first = async(submit)(iterator.next())
        try {
          await(slowStarted)
          assert(result(first) == 1)
        } finally {
          releaseSlow.countDown()
        }
        assert(iterator.toList == List(0))
      }
    }
  }

  test("cancelling a task removes its queued bins and drains only its active writers") {
    withResource(new GpuDeletionVectorWritePool(1)) { pool =>
      withProducers { submit =>
        val activeStarted = new CountDownLatch(1)
        val secondRead = new CountDownLatch(2)
        val releaseActive = new CountDownLatch(1)
        val otherAdmitted = new CountDownLatch(1)
        val writes = ArrayBuffer.empty[Int]
        val iterator = new GpuDeletionVectorWriteIterator(Iterator(0, 1).map { bin =>
          secondRead.countDown()
          bin
        }, pool, (bin: Int) => {
          writes.synchronized { writes += bin }
          activeStarted.countDown()
          await(releaseActive)
          Seq(bin)
        })
        val cancelled = async(submit)(iterator.toList)
        try {
          await(activeStarted)
          await(secondRead)
          val other = new GpuDeletionVectorWriteIterator(Iterator(100).map { bin =>
            otherAdmitted.countDown()
            bin
          }, pool, (bin: Int) => Seq(bin))
          val otherResult = async(submit)(other.toList)
          val closing = async(submit)(iterator.close())
          // The other task can acquire admission only after the queued bin is cancelled.
          await(otherAdmitted)
          assert(!closing.isDone, "close must wait for the active writer")
          releaseActive.countDown()
          result(closing)
          val error = intercept[ExecutionException](result(cancelled))
          assert(error.getCause.isInstanceOf[CancellationException])
          assert(result(otherResult) == List(100))
          assert(writes.synchronized { writes.toList } == List(0))
        } finally {
          releaseActive.countDown()
          iterator.close()
        }
      }
    }
  }

  test("worker failure cancels queued bins and waits for other active writers to close") {
    withResource(new GpuDeletionVectorWritePool(2)) { pool =>
      withProducers { submit =>
        val running = new CountDownLatch(2)
        val windowFull = new CountDownLatch(4)
        val fail = new CountDownLatch(1)
        val drain = new CountDownLatch(1)
        val boom = new IllegalStateException("synthetic DV write failure")
        val writes = ArrayBuffer.empty[Int]
        val closed = ArrayBuffer.empty[Int]
        val iterator = new GpuDeletionVectorWriteIterator((0 until 4).iterator.map { bin =>
          windowFull.countDown()
          bin
        }, pool, (bin: Int) => {
          withResource(new AutoCloseable {
            override def close(): Unit = closed.synchronized { closed += bin }
          }) { _ =>
            writes.synchronized { writes += bin }
            running.countDown()
            if (bin == 0) {
              await(fail)
              throw boom
            }
            await(drain)
            Seq(bin)
          }
        })
        val collected = async(submit)(iterator.toList)
        try {
          await(running)
          await(windowFull)
          fail.countDown()
          val other = new GpuDeletionVectorWriteIterator(Iterator(100), pool,
            (bin: Int) => Seq(bin))
          assert(result(async(submit)(other.toList)) == List(100))
          assert(!collected.isDone, "failure must drain the other active bin")
          drain.countDown()
          val error = intercept[ExecutionException](result(collected))
          assert(error.getCause eq boom)
          assert(writes.synchronized { writes.sorted.toList } == List(0, 1))
          assert(closed.synchronized { closed.sorted.toList } == List(0, 1))
        } finally {
          fail.countDown()
          drain.countDown()
          iterator.close()
        }
      }
    }
  }

  test("bin construction failure releases admission and drains previously submitted writes") {
    withResource(new GpuDeletionVectorWritePool(1)) { pool =>
      withProducers { submit =>
        val running = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        val boom = new IllegalStateException("synthetic bin construction failure")
        val bins = Iterator(0, 1).map { bin =>
          if (bin == 1) {
            await(running)
            throw boom
          }
          bin
        }
        val iterator = new GpuDeletionVectorWriteIterator(bins, pool, (bin: Int) => {
          running.countDown()
          await(release)
          Seq(bin)
        })
        val collected = async(submit)(iterator.toList)
        try {
          await(running)
          val otherAdmitted = new CountDownLatch(1)
          val other = new GpuDeletionVectorWriteIterator(Iterator(100).map { bin =>
            otherAdmitted.countDown()
            bin
          }, pool, (bin: Int) => Seq(bin))
          val otherResult = async(submit)(other.toList)
          await(otherAdmitted)
          assert(!collected.isDone)
          release.countDown()
          val error = intercept[ExecutionException](result(collected))
          assert(error.getCause eq boom)
          assert(result(otherResult) == List(100))
        } finally {
          release.countDown()
          iterator.close()
        }
      }
    }
  }

  test("a close failure prevents a bin's results from being returned") {
    withResource(new GpuDeletionVectorWritePool(1)) { pool =>
      val boom = new IllegalStateException("synthetic DV close failure")
      val iterator = new GpuDeletionVectorWriteIterator(Iterator(0), pool, (bin: Int) => {
        withResource(new AutoCloseable {
          override def close(): Unit = throw boom
        }) { _ => Seq(bin) }
      })
      assert(intercept[IllegalStateException](iterator.next()) eq boom)
    }
  }

  test("interrupting task cleanup still drains active writers and restores the interrupt") {
    withResource(new GpuDeletionVectorWritePool(1)) { pool =>
      withProducers { submit =>
        val running = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        val closingStarted = new CountDownLatch(1)
        val closingThread = new AtomicReference[Thread]
        val taskContext = TaskContext.empty()
        val previousTaskContext = TaskContext.get()
        val iterator = try {
          TaskContext.setTaskContext(taskContext)
          new GpuDeletionVectorWriteIterator(Iterator(0), pool, (bin: Int) => {
            running.countDown()
            await(release)
            Seq(bin)
          })
        } finally {
          TaskContext.setTaskContext(previousTaskContext)
        }
        val collected = async(submit)(iterator.toList)
        try {
          await(running)
          val closing = async(submit) {
            closingThread.set(Thread.currentThread())
            closingStarted.countDown()
            taskContext.markTaskCompleted(None)
            Thread.currentThread().isInterrupted
          }
          await(closingStarted)
          eventually(timeout(Span(10, Seconds))) {
            assert(closingThread.get().getState == Thread.State.WAITING)
          }
          closingThread.get().interrupt()
          assert(!closing.isDone)
          release.countDown()
          assert(result(closing), "cleanup must restore its caller's interrupt flag")
          val error = intercept[ExecutionException](result(collected))
          assert(error.getCause.isInstanceOf[CancellationException])
        } finally {
          release.countDown()
          iterator.close()
        }
      }
    }
  }

  test("workers propagate task context, user and classloader and restore them after a bin") {
    withResource(new GpuDeletionVectorWritePool(1)) { pool =>
      def workerContext(): (TaskContext, String, ClassLoader) = {
        val captured = new AtomicReference[(TaskContext, String, ClassLoader)]
        val finished = new CountDownLatch(1)
        pool.acquireAdmission(() => ())
        pool.enqueue(new GpuDeletionVectorWritePool.Work {
          override def cancelQueued(): Unit = fail("Context inspection was cancelled")

          override def run(): Unit = {
            try {
              captured.set((TaskContext.get(),
                UserGroupInformation.getCurrentUser.getUserName,
                Thread.currentThread().getContextClassLoader))
            } finally {
              pool.releaseAdmission()
              finished.countDown()
            }
          }
        }, () => ())
        await(finished)
        captured.get()
      }

      val baseline = workerContext()
      val taskContext = TaskContext.empty()
      val user = UserGroupInformation.createRemoteUser("dv-writer-test-user")
      val thread = Thread.currentThread()
      val previousTaskContext = TaskContext.get()
      val previousLoader = thread.getContextClassLoader
      val loader = new ClassLoader(previousLoader) {}
      val iterator = try {
        TaskContext.setTaskContext(taskContext)
        thread.setContextClassLoader(loader)
        user.doAs(new PrivilegedExceptionAction[GpuDeletionVectorWriteIterator[Int, Int]] {
          override def run(): GpuDeletionVectorWriteIterator[Int, Int] = {
            new GpuDeletionVectorWriteIterator(Iterator(0), pool, (bin: Int) => {
              assert(TaskContext.get() eq taskContext)
              assert(UserGroupInformation.getCurrentUser.getUserName == user.getUserName)
              assert(Thread.currentThread().getContextClassLoader eq loader)
              Seq(bin)
            })
          }
        })
      } finally {
        TaskContext.setTaskContext(previousTaskContext)
        thread.setContextClassLoader(previousLoader)
      }
      assert(iterator.toList == List(0))
      assert(workerContext() == baseline)
    }
  }
}
