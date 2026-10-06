/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from DMLWithDeletionVectorsHelper.scala and DeletionVectorStore.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

import java.nio.charset.StandardCharsets.UTF_8

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.spark.SparkContext
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.{DeletionVectorData, DeletionVectorResult,
  DeletionVectorWriter, FileToDvDescriptor}
import org.apache.spark.sql.delta.commands.DeletionVectorBitmapGenerator._
import org.apache.spark.sql.delta.commands.DeletionVectorWriter.DeletionVectorMapperContext
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArrayFormat
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.{DeletionVectorStore, HadoopFileSystemDVStore}
import org.apache.spark.sql.delta.util.{BinPackingIterator, PathWithFileSystem, Utils => DeltaUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations.absolutePath
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.util.{LongAccumulator, SerializableConfiguration, Utils => SparkUtils}

/** Executor totals in nanoseconds, measuring Hadoop stream calls rather than DML wall time. */
private[rapids] case class GpuDeletionVectorWriteMetrics(
    openTime: LongAccumulator,
    writeTime: LongAccumulator,
    closeTime: LongAccumulator) {
  def time[T](metric: LongAccumulator)(body: => T): T = {
    val start = System.nanoTime()
    try {
      body
    } finally {
      metric.add(System.nanoTime() - start)
    }
  }
}

private[rapids] object GpuDeletionVectorWriteMetrics {
  def create(sc: SparkContext): GpuDeletionVectorWriteMetrics = {
    // SQL metrics are filtered out of the stage/task accumulator display.
    GpuDeletionVectorWriteMetrics(
      sc.longAccumulator("DV file open time (ns)"),
      sc.longAccumulator("DV file write time (ns)"),
      sc.longAccumulator("DV file close time (ns)"))
  }
}

/** Keeps Delta's bitmap format and merge logic while supplying an instrumented storage mapper. */
private[rapids] object GpuDeletionVectorWriter {
  def buildDeletionVectors(
      spark: SparkSession,
      txn: GpuOptimisticTransactionBase,
      candidatesHaveDVs: Boolean,
      targetScan: GpuTargetScan,
      condition: Column,
      candidateFiles: Seq[AddFile],
      metrics: GpuDeletionVectorWriteMetrics): Seq[DeletionVectorResult] = {
    // Capture file paths before filtering, since the predicate can introduce a join.
    val matchedRows = targetScan.dataFrame
      .withColumn(FILE_NAME_COL, targetScan.filePathColumn)
      .filter(condition)
      .withColumn(ROW_INDEX_COL, targetScan.rowIndexColumn)
    val rowsWithDVs = if (candidatesHaveDVs) {
      val fileToDV = candidateFiles.map { add =>
        FileToDvDescriptor(
          SparkPath.fromPath(absolutePath(txn.deltaLog.dataPath.toString, add.path)).urlEncoded,
          Option(add.deletionVector).map(_.serializeToBase64()))
      }
      val descriptors = spark.createDataset(fileToDV)
      matchedRows.join(descriptors, descriptors("path") === matchedRows(FILE_NAME_COL), "leftOuter")
        .drop(FILE_NAME_COL)
        .withColumnRenamed("path", FILE_NAME_COL)
    } else {
      matchedRows.withColumn(FILE_DV_ID_COL, lit(null))
    }

    val bitmapAggregator = new BitmapAggregator(
      DFUDFShims.columnToExpr(col(ROW_INDEX_COL)), RoaringBitmapArrayFormat.Portable)
    val bitmapAndCount = "__delta_internal_bitmap_and_count"
    val aggregated = rowsWithDVs.groupBy(col(FILE_NAME_COL), col(FILE_DV_ID_COL))
      .agg(DFUDFShims.exprToColumn(bitmapAggregator.toAggregateExpression(isDistinct = false))
        .as(bitmapAndCount))
      .select(col(FILE_NAME_COL), col(FILE_DV_ID_COL),
        col(s"$bitmapAndCount.bitmap").as(DELETED_ROW_INDEX_BITMAP),
        col(s"$bitmapAndCount.cardinality").as(DELETED_ROW_INDEX_COUNT))

    val hadoopConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(txn.deltaLog.newDeltaHadoopConf()))
    val tablePathString = DeletionVectorStore.pathToEscapedString(txn.deltaLog.dataPath)
    val prefixLength = DeltaUtils.getRandomPrefixLength(txn.metadata)
    val packingTargetSize = spark.conf.get(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE)
    import DeletionVectorResult.encoder
    aggregated.as[DeletionVectorData].mapPartitions { rows =>
      val store = new GpuTimedDeletionVectorStore(hadoopConf.value.value, metrics)
      val tablePath = DeletionVectorStore.escapedStringToPath(tablePathString)
      val tablePathWithFS = store.pathWithFileSystem(tablePath)
      val pool = GpuDeletionVectorWritePool.get
      def writeBin(bin: Seq[DeletionVectorData]): Seq[DeletionVectorResult] = {
        val prefix = DeltaUtils.getRandomPrefix(prefixLength)
        val (writer, fileId) = DeletionVectorWriter.createWriter(store, tablePathWithFS, prefix)
        val context = DeletionVectorMapperContext(store, writer, tablePath, fileId, prefix)
        def writeRows(): Seq[DeletionVectorResult] =
          bin.map(row => DeletionVectorWriter.storeBitmapAndGenerateResult(context, row))
        if (pool.isDefined) {
          // A completed parallel bin must include a successful close on every Spark version.
          withResource(writer)(_ => writeRows())
        } else {
          SparkUtils.tryWithResource(writer)(_ => writeRows())
        }
      }
      val bins = new BinPackingIterator(rows, packingTargetSize)
      pool.map(new GpuDeletionVectorWriteIterator(bins, _, writeBin))
        .getOrElse(bins.flatMap(writeBin))
    }.collect().toSeq
  }
}

/** Reads use Delta's store; only output stream operations are instrumented. */
private[rapids] class GpuTimedDeletionVectorStore(
    hadoopConf: Configuration,
    metrics: GpuDeletionVectorWriteMetrics) extends HadoopFileSystemDVStore(hadoopConf) {

  override def createWriter(path: PathWithFileSystem): DeletionVectorStore.Writer = {
    new DeletionVectorStore.Writer {
      private var outputStream: FSDataOutputStream = _
      private var writtenBytes = 0L

      override def write(data: Array[Byte]): DeletionVectorStore.DVRangeDescriptor = {
        // Preserve lazy creation: no output file or open/close timings for empty DVs.
        if (outputStream == null) {
          outputStream = metrics.time(metrics.openTime) {
            path.fs.create(path.path, false)
          }
          metrics.time(metrics.writeTime) {
            outputStream.writeByte(DeletionVectorStore.DV_FILE_FORMAT_VERSION_ID_V1)
          }
          writtenBytes += 1
        }
        // Checksum computation is deliberately excluded from stream write time.
        val range = DeletionVectorStore.DVRangeDescriptor(
          offset = outputStream.size(),
          length = data.length,
          checksum = DeletionVectorStore.calculateChecksum(data))
        if (writtenBytes != range.offset) {
          throw DeltaErrors.deletionVectorSizeMismatch()
        }
        metrics.time(metrics.writeTime) {
          outputStream.writeInt(data.length)
          outputStream.write(data)
          outputStream.writeInt(range.checksum)
        }
        writtenBytes += DeletionVectorStore.getTotalSizeOfDVFieldsInFile(data.length)
        range
      }

      override val serializedPath: Array[Byte] =
        DeletionVectorStore.pathToEscapedString(path.path).getBytes(UTF_8)

      override def close(): Unit = {
        if (outputStream != null) {
          metrics.time(metrics.closeTime) {
            outputStream.close()
          }
        }
      }
    }
  }
}
