package com.nvidia.spark.rapids.fileio.memory;

import com.nvidia.spark.rapids.jni.Preconditions;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;

import java.io.IOException;
import java.util.Objects;

public class ByteArrayInputStream extends SeekableInputStream {
  private final byte[] buffer;
  private final int offset;
  private final int mark;
  private int pos;
  private boolean closed = false;

  public ByteArrayInputStream(byte[] buf) {
    this.buffer = Objects.requireNonNull(buf);
    this.offset = 0;
    this.mark = buf.length;
    this.pos = offset;
  }

  public ByteArrayInputStream(byte[] buf, int offset, int length) {
    this.buffer = Objects.requireNonNull(buf);
    this.offset = offset;
    this.mark = offset + length;
    this.pos = offset;

    Preconditions.ensure(offset >= 0, "Offset must be non-negative");
    Preconditions.ensure(offset < buf.length, "Offset must be less than buffer length");
    Preconditions.ensure(length >= 0, "Length must be non-negative");
    Preconditions.ensure(mark <= buf.length,
        "Offset + Length must be less than or equal to buffer length");
  }

  private void checkClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
  }

  @Override
  public long getPos() throws IOException {
    checkClosed();
    return pos;
  }

  @Override
  public void seek(long l) throws IOException {
    checkClosed();
    if (l < offset || l > mark) {
      throw new IOException("Seek position out of bounds: " + l);
    }
    if (l > Integer.MAX_VALUE) {
      throw new IOException("Seek position too large: " + l);
    }
    pos = (int) l;
  }

  @Override
  public int read() throws IOException {
    checkClosed();
    return pos < mark ? buffer[pos++] & 255 : -1;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkClosed();
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }

    if (pos >= mark) {
      return -1;
    } else {
      int avail = mark - pos;
      if (len > avail) {
        len = avail;
      }

      if (len <= 0) {
        return 0;
      } else {
        System.arraycopy(buffer, pos, b, off, len);
        pos += len;
        return len;
      }
    }
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
  }
}
