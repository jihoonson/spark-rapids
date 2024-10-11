package org.apache.spark.sql.rapids

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingDeque, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.{ExecutionContext, Future, Promise}

import org.apache.spark.util.ThreadUtils

//abstract class Task(val streamId: Int, val weight: Long, val onSuccess: Runnable = () => {})
//  extends Runnable {
//  val promise: Promise[Unit] = Promise[Unit]()
//}
//
//trait TrafficController[T <: Task] {
//  def canAccept(task: T): Boolean
//}
//
//class AlwaysAcceptTrafficController extends TrafficController[Task] {
//  override def canAccept(task: Task): Boolean = true
//}
//
//class WeightLimiter(val limitBytes: Long)
//  extends TrafficController[Task] {
//
//  private var inFlightBytes: Long = 0
//
//  override def canAccept(task: Task): Boolean = {
//    if (inFlightBytes + task.weight <= limitBytes) {
//      inFlightBytes += task.weight
//      true
//    } else {
//      false
//    }
//  }
//}

object AsyncWriter2 {
  val asyncOutputWriter = new AsyncWriter2(32)
}

class AsyncWriter2(val poolSize: Int, name: Option[String] = None) extends AutoCloseable {

  private val schedulerCtx = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool(name.getOrElse("rapids-async-write-scheduler"), 1))
  private val executorCtx = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool(name.getOrElse("rapids-async-writer"), poolSize))

  private class Stream(val id: Int) {
    val queue: LinkedBlockingDeque[Task] = new LinkedBlockingDeque[Task]()
    var lastError: Option[Throwable] = None
    var closed: Boolean = false
  }

  // TODO: currently 1-1 mapping between stream and executor, but should be fixed
  private val streams: ConcurrentHashMap[Int, Stream] =
    new ConcurrentHashMap[Int, Stream]()

  private val closed: AtomicBoolean = new AtomicBoolean(false)

  private var nextStreamId: AtomicInteger = new AtomicInteger(0)

  private def processTasks(stream: Stream): Unit = {
    while (!closed.get() && !stream.synchronized[Boolean](stream.closed)) {
      val task = stream.queue.poll(100, TimeUnit.MILLISECONDS)
      if (task != null) {
        try {
          task.run()
          task.promise.success({})
          task.onSuccess.run()
        } catch {
          case t: Throwable =>
            task.promise.failure(t)
            stream.synchronized[Unit](
              {
                stream.closed = true
                stream.lastError = Some(t)
                stream.queue.clear()
              }
            )
        }
      }
    }
  }

  def register(priority: Int = 0): Int = {
    val newStream = streams.compute(nextStreamId.getAndIncrement(), (k, v) => {
      if (v != null) {
        throw new IllegalStateException(s"Stream $nextStreamId already exists")
      }
      val s = new Stream(k)
      s
    })

    executorCtx.execute(() => processTasks(newStream))

    newStream.id
  }

  def deregister(streamId: Int): Unit = {
    val s = streams.remove(streamId)
    if (s == null) {
      throw new IllegalStateException(s"Stream $streamId does not exist")
    }
    // TODO: clean up stream
    s.synchronized[Unit]({
      s.closed = true
      // TODO: check if the queue was empty
      s.lastError = Some(new IllegalStateException("Stream closed"))
      s.queue.forEach(t =>
        t.promise.failure(new IllegalStateException("Stream closed")))
    })
  }

  def schedule(task: Task): Option[Future[Unit]] = {
    streams.get(task.streamId).queue.add(task)
    Some(task.promise.future)
  }

  // TODO: cancel

  def latestError(streamId: Int): Option[Throwable] = {
    val s = streams.get(streamId)
    if (s == null) {
      throw new IllegalStateException(s"Stream $streamId does not exist")
    }
    s.synchronized[Option[Throwable]](s.lastError)
  }

  override def close(): Unit = {
    if (!closed.compareAndSet(false, true)) {
      streams.values().forEach(s => {
        s.synchronized[Unit](
          {
            s.closed = true
            // TODO: check if the queue was empty
            s.lastError = Some(new IllegalStateException("AsyncWriter is closed"))
            s.queue.forEach(t =>
              t.promise.failure(new IllegalStateException("AsyncWriter is closed")))
          }
        )
      })
      schedulerCtx.shutdown()
      executorCtx.shutdown()
    }
  }
}
