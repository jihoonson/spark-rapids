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

package org.apache.spark.sql.delta.deletionvectors

import ai.rapids.cudf.HostMemoryBuffer

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.hadoop.fs.Path

case class RapidsDeletionVectorStoredBitmap(
    dvDescriptor: DeletionVectorDescriptor,
    tableDataPath: Path
) {
  require(dvDescriptor.isOnDisk, "Only on-disk deletion vectors are supported")

  def load(dvStore: RapidsDeletionVectorStore): HostMemoryBuffer = {
    val buffer = if (isEmpty) {
      RapidsDeletionVectorStoredBitmap.EMPTY_BITMAP
    } else {
      if (isInline) {
        throw new UnsupportedOperationException("Inline deletion vectors are not supported")
      } else {
        assert(isOnDisk)
        dvStore.load(onDiskPath, dvDescriptor.offset.getOrElse(0), dvDescriptor.sizeInBytes)
      }
    }

    buffer
  }

  def size: Int = dvDescriptor.sizeInBytes

  def cardinality: Long = dvDescriptor.cardinality

  lazy val getUniqueId: String = dvDescriptor.serializeToBase64()

  private def isEmpty: Boolean = dvDescriptor.isEmpty

  private def isInline: Boolean = dvDescriptor.isInline

  private def isOnDisk: Boolean = dvDescriptor.isOnDisk

  /** The absolute path for on-disk deletion vectors. */
  private lazy val onDiskPath: Path = dvDescriptor.absolutePath(tableDataPath)
}

object RapidsDeletionVectorStoredBitmap {

  /**
   * A serialized empty bitmap in host memory buffer. For details of the serialization format, see:
   * https://github.com/RoaringBitmap/RoaringFormatSpec/blob/8c4f7c7087c2a3a4fa560a34c669be673264f3ad/README.md#extension-for-64-bit-implementations
   */
  lazy val EMPTY_BITMAP: HostMemoryBuffer = {
    val buffer = HostMemoryBuffer.allocate(8)
    buffer.setLong(0, 0L)
    buffer
  }
}
