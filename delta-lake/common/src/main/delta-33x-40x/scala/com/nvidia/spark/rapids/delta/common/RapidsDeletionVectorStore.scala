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

package com.nvidia.spark.rapids.delta.common

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import java.io.DataInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class RapidsDeletionVectorStore(hadoopConf: Configuration) {

  def open(path: Path): DataInputStream = {
    val fs = path.getFileSystem(hadoopConf)
    fs.open(path)
  }

  def read(path: Path, offset: Int, size: Int): HostMemoryBuffer = {
    val fs = path.getFileSystem(hadoopConf)
    val serializedBitmap = HostMemoryBuffer.allocate(size)
    withResource(fs.open(path)) { reader =>
      reader.seek(offset)
      serializedBitmap.copyFromStream(0, reader, size)
    }
    serializedBitmap
  }
}