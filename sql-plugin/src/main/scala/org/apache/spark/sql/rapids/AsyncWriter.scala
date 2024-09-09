/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import java.io.{IOException, OutputStream}
import java.util.concurrent.locks.{Condition, ReentrantLock}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

import org.apache.spark.util.ThreadUtils

abstract class Task(val streamId: Int, val weight: Int = 1) extends Runnable {
  val promise: Promise[Unit] = Promise[Unit]()
}

// TODO: abstract it so that it can be used for writing HostMemoryBuffers? to perform the copy
// from native memory to java heap memory asynchronously.
// like, ByteBufferTask, HostMemoryBufferTask, etc.
class WriteStreamTask(val b: Array[Byte], val off: Int, val len: Int, streamId: Int,
    val sink: OutputStream) extends Task(streamId) {

  override def run(): Unit = {
    sink.write(b, off, len)
  }
}

/**
 * A service that performs write tasks asynchronously. Callers should register themselves before
 * they submit tasks. The service processes tasks based on their priority. When an error occurs,
 * all tasks submitted from the same caller are cancelled. The caller can wait until all its tasks
 * are done using {@link AsyncWriter#flush}. The caller can cancel all its tasks using
 * {@link AsyncWriter#cancelStream}. The caller must check the latest error before submitting new
 * tasks. The caller can clear the latest error using {@link AsyncWriter#clearLatestError}.
 *
 * TODO: It should be able to limit the total number of in-flight buffers across all callers.
 * TODO: It should support various prioritization strategies. The default is FIFO.
 * TODO: nvtx range or any metric for wait time.
 * TODO: what kind of metrics should be exposed?
 *   - # of tasks in the queue
 *   - in-flight bytes in the queue
 *   - # of tasks processed, maybe for testing
 */
object AsyncWriter extends AutoCloseable {

  // 1. FIFO processing of buffers
  // 2. the caller should be able to wait until all buffers are processed
  //   - I should know how many buffers are in-flight per caller
  // 3. the caller should be able to cancel all its tasks to close itself
  // 4. when a task fails, all subsequent tasks should be cancelled. and the error should be
  // returned to the caller
  //  - I should not accept new tasks anymore from the failed caller in this case

  private val poolSize = 1 // TODO: make it configurable
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("async-writer", poolSize))

  private val lock: ReentrantLock = new ReentrantLock()
  private val condition: Condition = lock.newCondition()

  /**
   * Tasks that have not been processed yet
   */
  @GuardedBy("lock")
  private val streamToTasks: mutable.Map[Int, mutable.Queue[Task]] =
    new mutable.HashMap[Int, mutable.Queue[Task]]()

//  @GuardedBy("this")
//  private val streamToFlushes: mutable.Map[Int, Promise[Unit]] =
//    new mutable.HashMap[Int, Promise[Unit]]()

  /**
   * Latest errors per stream
   */
  @GuardedBy("lock")
  private val streamToErrors: mutable.Map[Int, Throwable] =
    new mutable.HashMap[Int, Throwable]()

  /**
   * Tasks to process
   */
  @GuardedBy("lock")
  private val tasks = new mutable.Queue[Task]()

  @GuardedBy("lock")
  private var nextStreamId: Int = -1;

  @GuardedBy("lock")
  private var closed = false

  // TODO: throttling

  executionContext.execute(() => processBuffers())

  private def processBuffers(): Unit = {
    while (!isClosed) {
      // wait until buffers is not empty
      lock.lockInterruptibly()
      try {
        condition.await() // TODO: timeout?
      } finally {
        lock.unlock()
      }

      var continue: Boolean = true

      while (continue) {
        var task: Task = null
        var buffersFromMap: mutable.Queue[Task] = null
        lock.lockInterruptibly()
        try {
          continue = !closed && tasks.nonEmpty
          if (continue) {
            // TODO: I should release the lock and re-acquire it before processing the buffers
            task = tasks.dequeue()
            buffersFromMap = streamToTasks.getOrElseUpdate(task.streamId,
              throw new IllegalStateException(s"Stream id ${task.streamId} is not registered"))
            if (!task.equals(buffersFromMap.head)) {
              throw new IllegalStateException("Buffer is not the same as the one in the map")
            }

          }
        } finally {
          lock.unlock()
        }

        if (task != null) {
          try {
            System.err.println("processing")
            write(task)
            task.promise.success({})
          } catch {
            case t: Throwable =>
              lock.lockInterruptibly()
              try {
                streamToErrors.put(task.streamId, t)
                val targetId = task.streamId
                tasks.dequeueAll(eachTask => eachTask.streamId == targetId).foreach(eachTask => {
                  eachTask.promise.failure(
                    new RuntimeException("Failed because of previous error"))
                })
              } finally {
                lock.unlock()
              }
              task.promise.failure(t)
            // TODO: should I clear streamToTasks?
            //              streamToFlushes.remove(b.streamId).foreach(promise => promise.failure(
            //                new RuntimeException("Failed because of previous error")
            //              ))
          } finally {
            lock.lockInterruptibly()
            try {
              buffersFromMap.dequeue()
            } finally {
              lock.unlock()
            }
          }
        }

        //          if (buffersFromMap.isEmpty) {
        //            val maybeFlush = streamToFlushes.remove(b.streamId)
        //            maybeFlush match {
        //              case Some(promise) => promise.success({})
        //              case None =>
        //            }
        //          }
      }
    }
  }

  // TODO: rename this if non-stream caller is supported
  private def getNextStreamId: Int = {
    lock.lockInterruptibly()
    try {
      if (nextStreamId == Int.MaxValue) {
        nextStreamId = 0
      }
      nextStreamId += 1
      nextStreamId
    } finally {
      lock.unlock()
    }
  }

  // TODO: maybe rename to newId or something
  def register(): Int = {
    lock.lockInterruptibly()
    try {
      val streamId = getNextStreamId
      if (streamToTasks.contains(streamId)) {
        throw new IllegalStateException(s"Stream id $streamId is already registered")
      }
      streamToTasks.put(streamId, new mutable.Queue[Task]())

      streamId
    } finally {
      lock.unlock()
    }
  }

  def deregister(streamId: Int): Unit = {
    lock.lockInterruptibly()
    try {
      //    streamToFlushes.remove(streamId)
      streamToTasks.remove(streamId)
    } finally {
      lock.unlock()
    }
  }

  @throws[IOException]
  private def write(b: Task): Unit = {
    b.run()
  }

  // TODO: should this be blocking when the buffer size exceeds the limit?
  def schedule(task: Task): Option[Future[Unit]] = {
    lock.lockInterruptibly()
    System.err.println("scheduling")
    try {
      streamToErrors.get(task.streamId) match {
        case Some(t) =>
          task.promise.failure(t)
          return Some(task.promise.future)
        case None =>
      }

      // TODO: should check the in-flight buffer size and return None if it exceeds the limit
      val streamBuffers = streamToTasks.getOrElseUpdate(task.streamId,
        throw new IllegalStateException(s"Stream id $task.streamId is not registered"))
      streamBuffers += task
      tasks += task
      condition.signalAll()
      Some(task.promise.future)
    } finally {
      lock.unlock()
    }
  }

  @throws[IOException]
  def flush(streamId: Int): Future[Unit] = {
    lock.lockInterruptibly()
    try {
      // TODO: Should be able to flush all buffers associated with a particular sink
      // flush does not change processing order of the tasks.
      // the caller should wait until all tasks are done.
      val streamBuffers = streamToTasks.getOrElseUpdate(streamId,
        throw new IllegalStateException(s"Stream id $streamId is not registered"))
      System.err.println("flushing " + streamBuffers.size)
      if (streamBuffers.isEmpty) {
        Future.successful({})
      } else {
        streamBuffers.last.promise.future
      }
    } finally {
      lock.unlock()
    }
  }

  def latestError(streamId: Int): Option[Throwable] = {
    lock.lockInterruptibly()
    try {
      streamToTasks.getOrElseUpdate(streamId,
        throw new IllegalStateException(s"Stream id $streamId is not registered"))
      streamToErrors.get(streamId)
    } finally {
      lock.unlock()
    }
  }

  def clearLatestError(streamId: Int): Option[Throwable] = {
    lock.lockInterruptibly()
    try {
      streamToTasks.getOrElseUpdate(streamId,
        throw new IllegalStateException(s"Stream id $streamId is not registered"))
      streamToErrors.remove(streamId)
    } finally {
      lock.unlock()
    }
  }

  @throws[IOException]
  def flushAndCloseStream(streamId: Int): Unit = {
    // TODO: should be able to remove all buffers associated with a particular sink
    val f = flush(streamId)
    f.wait()
    deregister(streamId)
  }

  def cancelStream(streamId: Int): Unit = {
    lock.lockInterruptibly()
    try {
      tasks.dequeueAll(task => task.streamId == streamId).foreach(task => {
        task.promise.failure(new InterruptedException("Cancelled"))
      })
    } finally {
      lock.unlock()
    }
//    streamToFlushes.remove(streamId).foreach(promise => promise.failure(
//      new InterruptedException("Cancelled")
//    ))
  }

  @throws[IOException]
  def cancelAndCloseStream(streamId: Int): Unit = {
    cancelStream(streamId)
    deregister(streamId)
  }

  @throws[IOException]
  override def close(): Unit = {
    lock.lockInterruptibly()
    try {
      if (!closed) {
        closed = true
        tasks.clear()
        //      streamToFlushes.clear()
        streamToTasks.clear()
        condition.signalAll()

        executionContext.shutdown()
      }
    } finally {
      lock.unlock()
    }
  }

  def isClosed: Boolean = {
    lock.lockInterruptibly()
    try {
      closed
    } finally {
      lock.unlock()
    }
  }
}
