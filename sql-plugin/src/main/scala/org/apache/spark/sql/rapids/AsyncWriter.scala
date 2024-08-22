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
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

import org.apache.spark.util.ThreadUtils

// TODO: abstract it so that it can be used for writing HostMemoryBuffers? to perform the copy
// from native memory to java heap memory asynchronously.
class Task(
    val b: Array[Byte], val off: Int, val len: Int, val streamId: Int, val sink: OutputStream) {
  val promise: Promise[Unit] = Promise[Unit]()
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
 * TODO: nvtx range or any metric for waiting time
 */
object AsyncWriter extends AutoCloseable{

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

  @GuardedBy("this")
  private val streamToTasks: mutable.Map[Int, mutable.Queue[Task]] =
    new mutable.HashMap[Int, mutable.Queue[Task]]()

  @GuardedBy("this")
  private val streamToFlushes: mutable.Map[Int, Promise[Unit]] =
    new mutable.HashMap[Int, Promise[Unit]]()

  @GuardedBy("this")
  private val streamToErrors: mutable.Map[Int, Throwable] =
    new mutable.HashMap[Int, Throwable]()

  @GuardedBy("this")
  private val buffers = new mutable.Queue[Task]()

  @GuardedBy("this")
  private var nextStreamId: Int = -1;

  @GuardedBy("this")
  private var closed = false

  // TODO: throttling

  processBuffers()

  private def processBuffers(): Unit = {
    // wait until buffers is not empty
    synchronized {
      while (!closed) {
        wait() // TODO: timeout

        while (!closed && buffers.nonEmpty) {
          // TODO: I should release the lock and re-acquire it before processing the buffers
          val b = buffers.dequeue()
          val buffersFromMap = streamToTasks.getOrElseUpdate(b.streamId,
            throw new IllegalStateException(s"Stream id ${b.streamId} is not registered"))
          if (!b.equals(buffersFromMap.head)) {
            throw new IllegalStateException("Buffer is not the same as the one in the map")
          }

          try {
            write(b)
            // Dequeue only after successful write
            buffersFromMap.dequeue()
            b.promise.success({})
          } catch {
            case t: Throwable =>
              streamToErrors.put(b.streamId, t)
              b.promise.failure(t)
              buffers.dequeueAll(task => task.streamId == b.streamId).foreach(task => {
                task.promise.failure(new RuntimeException("Failed because of previous error"))
              })
              // TODO: should I clear streamToTasks?
              streamToFlushes.remove(b.streamId).foreach(promise => promise.failure(
                new RuntimeException("Failed because of previous error")
              ))
          }

          if (buffersFromMap.isEmpty) {
            val maybeFlush = streamToFlushes.remove(b.streamId)
            maybeFlush match {
              case Some(promise) => promise.success({})
              case None =>
            }
          }
        }
      }
    }
  }

  private def getNextStreamId: Int = {
    if (nextStreamId == Int.MaxValue) {
      nextStreamId = 0
    }
    nextStreamId += 1
    nextStreamId
  }

  def register(): Int = synchronized {
    val streamId = getNextStreamId
    if (streamToTasks.contains(streamId)) {
      throw new IllegalStateException(s"Stream id $streamId is already registered")
    }
    streamToTasks.put(streamId, new mutable.Queue[Task]())

    streamId
  }

  def deregister(streamId: Int): Unit = synchronized {
    streamToFlushes.remove(streamId)
    streamToTasks.remove(streamId)
  }

  @throws[IOException]
  private def write(b: Task): Unit = {
    b.sink.write(b.b, b.off, b.len)
  }

  def schedule(streamId: Int, b: Task): Option[Future[Unit]] = synchronized {
    if (streamToErrors.contains(streamId)) {
      b.promise.failure(streamToErrors(streamId))
      return Some(b.promise.future)
    }
    // TODO: should check the in-flight buffer size and return None if it exceeds the limit

    val streamBuffers = streamToTasks.getOrElseUpdate(streamId,
      throw new IllegalStateException(s"Stream id $streamId is not registered"))
    streamBuffers += b
    buffers += b
    notifyAll()
    Some(b.promise.future)
  }

  @throws[IOException]
  def flush(streamId: Int): Future[Unit] = synchronized {
    // TODO: Should be able to flush all buffers associated with a particular sink
    // flush does not change processing order of the tasks.
    // the caller should wait until all tasks are done.
    val streamBuffers = streamToTasks.getOrElseUpdate(streamId,
      throw new IllegalStateException(s"Stream id $streamId is not registered"))
    if (streamBuffers.isEmpty) {
      Future.successful({})
    } else {
      val promise = streamToFlushes.getOrElseUpdate(streamId, Promise[Unit]())
      promise.future
    }
  }

  def latestError(streamId: Int): Option[Throwable] = synchronized {
    streamToTasks.getOrElseUpdate(streamId,
      throw new IllegalStateException(s"Stream id $streamId is not registered"))
    streamToErrors.get(streamId)
  }

  def clearLatestError(streamId: Int): Option[Throwable] = synchronized {
    streamToTasks.getOrElseUpdate(streamId,
      throw new IllegalStateException(s"Stream id $streamId is not registered"))
    streamToErrors.remove(streamId)
  }

  @throws[IOException]
  def flushAndCloseStream(streamId: Int): Unit = {
    // TODO: should be able to remove all buffers associated with a particular sink
    val f = flush(streamId)
    f.wait()
    deregister(streamId)
  }

  def cancelStream(streamId: Int): Unit = synchronized {
    buffers.dequeueAll(task => task.streamId == streamId).foreach(task => {
      task.promise.failure(new InterruptedException("Cancelled"))
    })
    streamToFlushes.remove(streamId).foreach(promise => promise.failure(
      new InterruptedException("Cancelled")
    ))
  }

  @throws[IOException]
  def cancelAndCloseStream(streamId: Int): Unit = {
    cancelStream(streamId)
    deregister(streamId)
  }

  @throws[IOException]
  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      buffers.clear()
      streamToFlushes.clear()
      streamToTasks.clear()
      notifyAll()

      executionContext.shutdown()
    }
  }
}
