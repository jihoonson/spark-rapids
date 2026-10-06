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

package org.apache.spark.sql.delta.rapids

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{ArrayBlockingQueue, CancellationException, Executors,
  LinkedBlockingQueue, RejectedExecutionException, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.RapidsConf
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.delta.util.threads.CapturedSparkThreadLocals

private[rapids] object GpuDeletionVectorWritePool {
  // Check cancellation while waiting for admission, queued work, or completed results.
  val pollIntervalMillis: Long = 100L

  private[rapids] trait Work extends Runnable {
    def cancelQueued(): Unit
  }

  // A new SparkEnv also permits local-mode applications to use different startup settings.
  private var sparkEnv: SparkEnv = _
  private var pool: Option[GpuDeletionVectorWritePool] = None
  private var shutdownHookRegistered = false

  def get: Option[GpuDeletionVectorWritePool] = synchronized {
    val currentEnv = SparkEnv.get
    if (currentEnv ne sparkEnv) {
      pool.foreach(_.close())
      sparkEnv = currentEnv
      val threads = new RapidsConf(currentEnv.conf).deltaDeletionVectorWriteNumThreads
      pool = if (threads == 0) None else Some(new GpuDeletionVectorWritePool(threads))
      if (pool.nonEmpty && !shutdownHookRegistered) {
        Runtime.getRuntime.addShutdownHook(new Thread("rapids-dv-writer-shutdown") {
          override def run(): Unit = GpuDeletionVectorWritePool.synchronized {
            pool.foreach(_.close())
          }
        })
        shutdownHookRegistered = true
      }
    }
    pool
  }
}

/** Fair executor-wide admission, with N workers and N queued packed bins. */
private[rapids] class GpuDeletionVectorWritePool(val numThreads: Int) extends AutoCloseable {
  import GpuDeletionVectorWritePool.{pollIntervalMillis, Work}

  require(numThreads > 0 && numThreads <= Int.MaxValue / 2)
  val maxInFlight: Int = 2 * numThreads
  private val admission = new Semaphore(maxInFlight, true)
  private val queueSlots = new Semaphore(numThreads, true)
  private val queue = new ArrayBlockingQueue[Work](numThreads)
  private val stateLock = new Object
  @volatile private var stopped = false
  private val workers = Executors.newFixedThreadPool(numThreads,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("rapids-dv-writer-%d").build())

  // The executor receives only these N consumers; bin payloads live in the bounded queue.
  (0 until numThreads).foreach { _ =>
    workers.execute(new Runnable {
      override def run(): Unit = {
        while (!stopped || !queue.isEmpty) {
          try {
            val work = queue.poll(pollIntervalMillis, TimeUnit.MILLISECONDS)
            if (work != null) {
              queueSlots.release()
              work.run()
            }
          } catch {
            // A backend can leave an interrupted worker behind after reporting an I/O error.
            case _: InterruptedException =>
          }
        }
      }
    })
  }

  private def acquire(semaphore: Semaphore, checkTask: () => Unit): Unit = {
    var acquired = false
    while (!acquired) {
      checkTask()
      if (stopped) {
        throw new RejectedExecutionException("Deletion vector writer pool is stopped")
      }
      acquired = semaphore.tryAcquire(pollIntervalMillis, TimeUnit.MILLISECONDS)
    }
  }

  def acquireAdmission(checkTask: () => Unit): Unit = acquire(admission, checkTask)

  def releaseAdmission(): Unit = admission.release()

  def enqueue(work: Work, checkTask: () => Unit): Unit = {
    acquire(queueSlots, checkTask)
    try {
      checkTask()
      stateLock.synchronized {
        if (stopped) {
          throw new RejectedExecutionException("Deletion vector writer pool is stopped")
        }
        queue.add(work)
      }
    } catch {
      case t: Throwable =>
        queueSlots.release()
        throw t
    }
  }

  def removeQueued(work: Work): Unit = {
    if (queue.remove(work)) {
      queueSlots.release()
    }
  }

  override def close(): Unit = {
    stateLock.synchronized {
      stopped = true
    }
    queue.iterator().asScala.toVector.foreach(_.cancelQueued())
    // Active bins retain their workers through close. Daemon consumers exit after draining.
    workers.shutdown()
  }
}

/** One task's bounded window of bin writes, yielding results only after worker cleanup. */
private[rapids] class GpuDeletionVectorWriteIterator[A, B](
    bins: Iterator[A],
    pool: GpuDeletionVectorWritePool,
    writeBin: A => Seq[B]) extends Iterator[B] with AutoCloseable {
  private val taskContext = TaskContext.get()
  private val sparkLocals = CapturedSparkThreadLocals()
  private val user = UserGroupInformation.getCurrentUser
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val jobs = mutable.Set.empty[BinWrite]
  private val completions = new LinkedBlockingQueue[Either[Throwable, Seq[B]]]()
  private var closed = false
  private var failure: Throwable = _
  private var outstanding = 0
  private var current: Iterator[B] = Iterator.empty

  Option(taskContext).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  private def checkTask(): Unit = {
    Option(taskContext).foreach(_.killTaskIfInterrupted())
    synchronized {
      if (failure != null) {
        throw failure
      }
      if (closed) {
        throw new CancellationException("Deletion vector writes are closed")
      }
    }
  }

  private def withTaskContext[T](body: => T): T = {
    val thread = Thread.currentThread()
    val previousLoader = thread.getContextClassLoader
    thread.setContextClassLoader(classLoader)
    try {
      user.doAs(new PrivilegedExceptionAction[T] {
        override def run(): T = sparkLocals.runWithCaptured(body)
      })
    } finally {
      thread.setContextClassLoader(previousLoader)
    }
  }

  private class BinWrite(private var body: () => Seq[B])
      extends GpuDeletionVectorWritePool.Work {
    // A queued job can be cancelled; a running job always owns its writer until close.
    private val queued = 0
    private val running = 1
    private val cancelled = 2
    private val state = new AtomicInteger(queued)

    override def run(): Unit = {
      if (state.compareAndSet(queued, running)) {
        var result: Seq[B] = null
        var error: Throwable = null
        try {
          if (GpuDeletionVectorWriteIterator.this.synchronized { !closed && failure == null }) {
            result = withTaskContext(body())
          }
        } catch {
          case t: Throwable => error = t
        } finally {
          finish(result, error)
        }
      }
    }

    override def cancelQueued(): Unit = {
      if (state.compareAndSet(queued, cancelled)) {
        pool.removeQueued(this)
        finish(null, new CancellationException("Queued deletion vector bin was cancelled"))
      }
    }

    private def finish(result: Seq[B], error: Throwable): Unit = {
      // Drop serialized bitmaps before allowing another producer to build a bin.
      body = null
      val cancel = GpuDeletionVectorWriteIterator.this.synchronized {
        jobs -= this
        val firstFailure = !closed && error != null && failure == null
        if (firstFailure) {
          failure = error
          completions.add(Left(error))
        } else if (!closed && failure == null && result != null) {
          completions.add(Right(result))
        }
        pool.releaseAdmission()
        GpuDeletionVectorWriteIterator.this.notifyAll()
        if (firstFailure) jobs.toVector else Seq.empty
      }
      cancel.foreach(_.cancelQueued())
    }
  }

  private def fillWindow(): Unit = {
    while (outstanding < pool.maxInFlight && bins.hasNext) {
      pool.acquireAdmission(() => checkTask())
      var job: BinWrite = null
      try {
        checkTask()
        val bin = bins.next()
        job = new BinWrite(() => writeBin(bin))
        synchronized {
          checkTask()
          jobs += job
        }
        pool.enqueue(job, () => checkTask())
        outstanding += 1
      } catch {
        case t: Throwable =>
          if (job == null) pool.releaseAdmission() else job.cancelQueued()
          throw t
      }
    }
  }

  private def guarded[T](body: => T): T = {
    try {
      body
    } catch {
      case t: Throwable =>
        close()
        throw t
    }
  }

  override def hasNext: Boolean = guarded {
    if (synchronized { closed }) {
      false
    } else {
      checkTask()
      while (!current.hasNext && (outstanding > 0 || bins.hasNext)) {
        fillWindow()
        var completion: Either[Throwable, Seq[B]] = null
        while (completion == null) {
          checkTask()
          completion = completions.poll(
            GpuDeletionVectorWritePool.pollIntervalMillis, TimeUnit.MILLISECONDS)
        }
        outstanding -= 1
        completion match {
          case Right(result) => current = result.iterator
          case Left(error) => throw error
        }
      }
      val more = current.hasNext
      if (!more) {
        close()
      }
      more
    }
  }

  override def next(): B = {
    if (!hasNext) {
      throw new NoSuchElementException("No more deletion vector write results")
    }
    current.next()
  }

  override def close(): Unit = {
    val pending = synchronized {
      closed = true
      jobs.toVector
    }
    pending.foreach(_.cancelQueued())
    // Interrupting a wait must not release task ownership while a writer is still open.
    var interrupted = Thread.interrupted()
    synchronized {
      while (jobs.nonEmpty) {
        try {
          wait()
        } catch {
          case _: InterruptedException => interrupted = true
        }
      }
    }
    completions.clear()
    current = Iterator.empty
    if (interrupted) {
      Thread.currentThread().interrupt()
    }
  }
}
