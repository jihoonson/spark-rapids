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

/**
 * Proxy to AsyncWriter
 *
 * TODO: not thread-safe
 */
class AsyncOutputStream(val delegate: OutputStream) extends OutputStream {
  private val streamId = AsyncWriter.register()

  private var closed = false

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

    val scheduleResult = AsyncWriter.schedule(
      new WriteStreamTask(b, off, len, streamId, delegate))
    scheduleResult match {
      case None => // TODO: wait and retry
      case _ => // do nothing
    }
  }

  @throws[IOException]
  override def flush(): Unit = {
    throwIfError()
    ensureOpen()

    AsyncWriter.flush(streamId)
  }

  @throws[IOException]
  override def close(): Unit = {
    throwIfError()

    if (!closed) {
      AsyncWriter.flushAndCloseStream(streamId)
      closed = true
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
    AsyncWriter.latestError(streamId) match {
      case Some(t) if t.isInstanceOf[IOException] => throw t
      case Some(t) => throw new IOException(t)
      case None =>
    }
  }
}
