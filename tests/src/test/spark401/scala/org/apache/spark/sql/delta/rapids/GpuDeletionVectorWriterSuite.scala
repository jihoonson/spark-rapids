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

import java.io.IOException
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.commands.DeletionVectorWriter
import org.apache.spark.sql.delta.commands.DeletionVectorWriter.DeletionVectorMapperContext
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.util.{LongAccumulator, Utils => SparkUtils}

class GpuDeletionVectorWriterSuite extends AnyFunSuite {
  private def newMetrics(): GpuDeletionVectorWriteMetrics = {
    GpuDeletionVectorWriteMetrics(
      new LongAccumulator, new LongAccumulator, new LongAccumulator)
  }

  private def withStore(
      body: (GpuTimedDeletionVectorStore, Path, GpuDeletionVectorWriteMetrics) => Unit): Unit = {
    val dir = Files.createTempDirectory("rapids-dv-writer")
    val path = new Path(dir.resolve("dv.bin").toUri)
    val metrics = newMetrics()
    val store = new GpuTimedDeletionVectorStore(new Configuration(), metrics)
    try {
      body(store, path, metrics)
    } finally {
      path.getFileSystem(new Configuration()).delete(new Path(dir.toUri), true)
    }
  }

  test("an unused writer does not create a file or record stream timings") {
    withStore { (store, path, metrics) =>
      SparkUtils.tryWithResource(store.createWriter(store.pathWithFileSystem(path))) { _ =>
        assert(!path.getFileSystem(new Configuration()).exists(path))
      }
      assert(metrics.openTime.value == 0)
      assert(metrics.writeTime.value == 0)
      assert(metrics.closeTime.value == 0)
    }
  }

  test("multiple DVs share one file and remain readable by Delta") {
    withStore { (store, path, metrics) =>
      val first = new RoaringBitmapArray()
      first.add(1L)
      first.add(100L)
      val second = new RoaringBitmapArray()
      second.add(5L)
      val firstBytes = first.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
      val secondBytes = second.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
      val ranges = SparkUtils.tryWithResource(
        store.createWriter(store.pathWithFileSystem(path))) { writer =>
        val firstRange = writer.write(firstBytes)
        val openTime = metrics.openTime.value
        val secondRange = writer.write(secondBytes)
        assert(metrics.openTime.value == openTime, "the second DV must reuse the stream")
        assert(metrics.closeTime.value == 0)
        (firstRange, secondRange)
      }
      assert(ranges._1.offset == 1)
      assert(ranges._2.offset ==
        1 + DeletionVectorStore.getTotalSizeOfDVFieldsInFile(firstBytes.length))
      assert(store.read(path, ranges._1.offset, ranges._1.length).contains(100L))
      assert(store.read(path, ranges._2.offset, ranges._2.length).contains(5L))
      assert(metrics.openTime.value > 0)
      assert(metrics.writeTime.value > 0)
      assert(metrics.closeTime.value > 0)
    }
  }

  test("failed file creation is timed and does not overwrite an existing DV file") {
    withStore { (store, path, metrics) =>
      Files.write(java.nio.file.Paths.get(path.toUri), Array[Byte](42))
      SparkUtils.tryWithResource(store.createWriter(store.pathWithFileSystem(path))) { writer =>
        intercept[IOException] {
          writer.write(Array[Byte](1, 2, 3))
        }
      }
      assert(Files.readAllBytes(java.nio.file.Paths.get(path.toUri)).sameElements(Array[Byte](42)))
      assert(metrics.openTime.value > 0)
      assert(metrics.writeTime.value == 0)
      assert(metrics.closeTime.value == 0)
    }
  }

  test("executor mapper merges existing DVs and exposes timings in stage and task accumulators") {
    withStore { (store, path, _) =>
      val spark = SparkSession.builder().master("local[2]")
        .appName("DV writer metrics test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
      try {
        val tablePath = path.getParent
        val bitmap = new RoaringBitmapArray()
        bitmap.add(1L)
        val (writer, fileId) = DeletionVectorWriter.createWriter(
          store, store.pathWithFileSystem(tablePath))
        val existingDV = SparkUtils.tryWithResource(writer) { _ =>
          DeletionVectorWriter.storeSerializedBitmap(
            DeletionVectorMapperContext(store, writer, tablePath, fileId, ""),
            bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable), bitmap.cardinality)
        }
        val addFile = AddFile("part.parquet", Map.empty, 100L, 0L, dataChange = true,
          deletionVector = existingDV)
        val deltaLog = mock[DeltaLog]
        when(deltaLog.dataPath).thenReturn(tablePath)
        when(deltaLog.newDeltaHadoopConf()).thenReturn(new Configuration())
        val txn = mock[GpuOptimisticTransactionBase]
        when(txn.deltaLog).thenReturn(deltaLog)
        when(txn.metadata).thenReturn(Metadata())

        import spark.implicits._
        val dataFilePath = new Path(tablePath, addFile.path).toUri.toString
        val rows = Seq((dataFilePath, 5L), (dataFilePath, 5L), (dataFilePath, 100L))
          .toDF("file", "rowIndex")
        val scan = GpuTargetScan(rows, col("file"), col("rowIndex"))
        val metrics = GpuDeletionVectorWriteMetrics.create(spark.sparkContext)
        val results = GpuDeletionVectorWriter.buildDeletionVectors(
          spark, txn, candidatesHaveDVs = true, scan, lit(true), Seq(addFile), metrics)
        assert(results.size == 1)
        assert(results.head.matchedRowCount == 2)
        assert(results.head.deletionVector.cardinality == 3)
        val merged = store.read(results.head.deletionVector, tablePath)
        assert(Seq(1L, 5L, 100L).forall(merged.contains))
        assert(metrics.openTime.value > 0)
        assert(metrics.writeTime.value > 0)
        assert(metrics.closeTime.value > 0)

        spark.sparkContext.listenerBus.waitUntilEmpty(10000)
        val stages = spark.sparkContext.statusStore.stageList(null)
        Seq(metrics.openTime, metrics.writeTime, metrics.closeTime).foreach { metric =>
          val stageUpdates = stages.flatMap(_.accumulatorUpdates).filter(_.id == metric.id)
          assert(stageUpdates.nonEmpty, s"${metric.name} must be visible in stage details")
          assert(stageUpdates.forall(_.name == metric.name.get))
          assert(stageUpdates.map(_.value.toLong).max == metric.value)
          val taskUpdates = stages.flatMap { stage =>
            spark.sparkContext.statusStore.taskList(stage.stageId, stage.attemptId, 100)
              .flatMap(_.accumulatorUpdates)
          }.filter(_.id == metric.id)
          assert(taskUpdates.nonEmpty, s"${metric.name} must be visible in the Tasks table")
          assert(taskUpdates.flatMap(_.update).map(_.toLong).sum == metric.value)
        }

        val emptyMetrics = GpuDeletionVectorWriteMetrics.create(spark.sparkContext)
        assert(GpuDeletionVectorWriter.buildDeletionVectors(
          spark, txn, candidatesHaveDVs = false, scan, lit(false), Seq(addFile), emptyMetrics)
          .isEmpty)
        assert(emptyMetrics.openTime.value == 0)
        assert(emptyMetrics.writeTime.value == 0)
        assert(emptyMetrics.closeTime.value == 0)
      } finally {
        spark.stop()
      }
    }
  }
}
