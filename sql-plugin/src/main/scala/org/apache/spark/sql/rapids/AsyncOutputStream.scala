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

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Proxy to AsyncWriter
 *
 * TODO: not thread-safe
 */
class AsyncOutputStream(val delegate: OutputStream) extends OutputStream {
  // TODO: should take AsyncWriter as a parameter
  private val asyncWriter = AsyncWriter2.asyncOutputWriter
  private val streamId = asyncWriter.register()

  private var closed = false

  private class Metrics {
    var numBytesScheduled: Long = 0
    var numBytesWritten: Long = 0
  }

  private val metrics = new Metrics

  private var flushTaskId: Int = 0

  private def newTaskId: Int = {
    flushTaskId += 1
    flushTaskId
  }

  private class WriteTask(val b: Array[Byte], val off: Int, val len: Int, streamId: Int,
      taskId: Int)
    extends Task(streamId, len, () => {
      metrics.numBytesWritten += len
//      System.err.println("written for taskId: " + taskId)
    }) {

    override def run(): Unit = {
//      System.err.println("flushing for taskId: " + taskId)
      delegate.write(b, off, len)
    }
  }

  private class FlushTask(streamId: Int) extends Task(streamId, 0) {

    override def run(): Unit = {
      delegate.flush()
    }
  }

  override def write(b: Int): Unit = {
    throwIfError()
    ensureOpen()

    val buffer = new Array[Byte](1)
    buffer(0) = b.toByte
    write(buffer)
  }

  @throws[IOException]
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    throwIfError()
    ensureOpen()

    metrics.numBytesScheduled += len
    val scheduleResult = asyncWriter.schedule(new WriteTask(b, off, len, streamId, newTaskId))
    scheduleResult match {
      case None => // TODO: wait and retry
      case _ => // do nothing
    }
  }

  @throws[IOException]
  override def flush(): Unit = {
    throwIfError()
    ensureOpen()

    val scheduleResult = asyncWriter.schedule(new FlushTask(streamId))
    scheduleResult match {
      case None => throw new IOException("Failed to schedule flush")
      case Some(flushing) => Await.ready(flushing, Duration.Inf) // TODO: maybe timeout
    }
  }

  @throws[IOException]
  override def close(): Unit = {
    throwIfError()

    if (!closed) {
      flush()
      asyncWriter.deregister(streamId)
      delegate.close() // TODO: safeClose
      closed = true
//      System.err.println("numBytesScheduled: " + metrics.numBytesScheduled)
//      System.err.println("numBytesWritten: " + metrics.numBytesWritten)
    }
  }

  @throws[IOException]
  private def ensureOpen(): Unit = {
    if (closed) {
      throw new IOException("Stream closed")
    }
  }

  @throws[IOException]
  private def throwIfError(): Unit = {
    asyncWriter.latestError(streamId) match {
      case Some(t) if t.isInstanceOf[IOException] => throw t
      case Some(t) => throw new IOException(t)
      case None =>
    }
  }
}
