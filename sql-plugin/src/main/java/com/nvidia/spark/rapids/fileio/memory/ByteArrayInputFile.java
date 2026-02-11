package com.nvidia.spark.rapids.fileio.memory;

import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;

import java.io.IOException;

public class ByteArrayInputFile implements RapidsInputFile {

  private final byte[] data;

  public ByteArrayInputFile(byte[] data) {
    this.data = data;
  }

  @Override
  public long getLength() throws IOException {
    return data.length;
  }

  @Override
  public SeekableInputStream open() throws IOException {
    return new ByteArrayInputStream(data);
  }
}
