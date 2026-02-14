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
import com.nvidia.spark.rapids.Arm.withResource
import java.io.{DataInputStream, IOException}

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * RAPIDS version of [[DeletionVectorStore]]. It is simplified to only include the APIs needed
 * for loading serialized deletion vectors into host memory.
 */
trait RapidsDeletionVectorStore {

  /**
   * Reads a serialized deletion vector from the given path and offset,
   * and loads it into a HostMemoryBuffer.
   */
  def load(path: Path, offset: Int, size: Int): HostMemoryBuffer
}

object RapidsDeletionVectorStore {
  def createInstance(hadoopConf: Configuration): RapidsDeletionVectorStore = {
    new RapidsHadoopDVStore(hadoopConf)
  }
}

class RapidsHadoopDVStore(hadoopConf: Configuration) extends RapidsDeletionVectorStore{

  override def load(path: Path, offset: Int, size: Int): HostMemoryBuffer = {
    val fs = path.getFileSystem(hadoopConf)
    withResource(fs.open(path)) { in =>
      in.seek(offset)
      DeltaSerializedBitmapLoader.load(in, size)
    }
  }
}

/**
 * Trait for the "Delta" roaring bitmap serialization format loaders. Delta support two
 * serialization formats for roaring bitmaps: "portable" and "native".
 * See [[RoaringBitmapArraySerializationFormat]] for details.
 */
trait DeltaSerializedBitmapLoader {
  def loadAsStandardFormat(input: DataInputStream, size: Int): HostMemoryBuffer
}

object DeltaSerializedBitmapLoader {
  /**
   * The "Delta" roaring bitmap serialization formats begin with a 4-byte magic number.
   * When converting to the "standard" roaring bitmap serialization format, this magic number
   * should be stripped. For details, see:
   * https://github.com/delta-io/delta/blob/ccd3092da05a68027bf9be9ec4273a810b4b9ef3/spark/src/main/scala/org/apache/spark/sql/delta/deletionvectors/RoaringBitmapArray.scala#L512-L515
   */
  val DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE = 4

  /**
   * Reads the given input stream and loads the serialized bitmap into a HostMemoryBuffer.
   * The input stream is expected to be in one of the two "Delta" roaring bitmap serialization
   * formats: "portable" or "native". The format is determined by reading the magic number at the
   * current position of the input stream. The bitmap is then loaded and converted to the
   * "standard" roaring bitmap serialization format, and returned as a HostMemoryBuffer.
   */
  def load(input: DataInputStream, size: Int): HostMemoryBuffer = {
    // The bitmap size is stored in big endian.
    // See DeletionVectorStore.readRangeFromStream for details.
    val sizeAccordingToFile = input.readInt()
    if (size != sizeAccordingToFile) {
      throw DeltaErrors.deletionVectorSizeMismatch()
    }

    val magicNumberBuf = HostMemoryBuffer.allocate(DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE, false)
    magicNumberBuf.copyFromStream(0, input, DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE)
    val magicNumber = magicNumberBuf.getInt(0)
    val remainingSize = size - DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE

    magicNumber match {
      case PortableRoaringBitmapArraySerializationFormat.MAGIC_NUMBER =>
        DeltaPortableFormatLoader.loadAsStandardFormat(input, remainingSize)
      case NativeRoaringBitmapArraySerializationFormat.MAGIC_NUMBER =>
        DeltaNativeFormatLoader.loadAsStandardFormat(input, remainingSize)
      case _ =>
        throw new IOException(s"Unexpected RoaringBitmapArray magic number $magicNumber")
    }
  }
}

object DeltaPortableFormatLoader extends DeltaSerializedBitmapLoader {

  override def loadAsStandardFormat(input: DataInputStream, size: Int): HostMemoryBuffer = {
    // The Delta portable format is identical to the standard portable format except for the
    // magic number at the beginning, which is already stripped at this point. Therefore,
    // we can directly load the remaining bytes into a HostMemoryBuffer and return it.
    val buffer = HostMemoryBuffer.allocate(size)
    buffer.copyFromStream(0, input, size)

    // TODO: checksum check. see DeletionVectorStore.readRangeFromStream for details
    buffer
  }
}

object DeltaNativeFormatLoader extends DeltaSerializedBitmapLoader {

  override def loadAsStandardFormat(input: DataInputStream, size: Int): HostMemoryBuffer = {
    // The Delta native format is not compatible with the standard portable format, so we
    // load the bitmap into a RoaringBitmapArray first, then re-serialize it in the standard
    // portable format.
    val originalBytes = readRangeFromStream(input, size)
    val roaringBitmapArray = RoaringBitmapArray.readFrom(originalBytes)
    val reserialized = roaringBitmapArray.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    val magicNumberSize = DeltaSerializedBitmapLoader.DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE
    val buffer = HostMemoryBuffer.allocate(reserialized.length - magicNumberSize)
    buffer.setBytes(0, reserialized, magicNumberSize, reserialized.length - magicNumberSize)
    buffer
  }

  /**
   * Migrated from DeletionVectorStore.readRangeFromStream and slightly modified.
   * This version does not read the bitmap size from the stream since that is already read
   * by the caller ([[DeltaSerializedBitmapLoader.read]]).
   */
  private def readRangeFromStream(reader: DataInputStream, size: Int): Array[Byte] = {
    val buffer = new Array[Byte](size)
    reader.readFully(buffer)

    val expectedChecksum = reader.readInt()
    val actualChecksum = DeletionVectorStore.calculateChecksum(buffer)
    if (expectedChecksum != actualChecksum) {
      throw DeltaErrors.deletionVectorChecksumMismatch()
    }

    buffer
  }
}
