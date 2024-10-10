package org.apache.spark.sql.rapids;

import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class AsyncOutputStreamBenchmark {
  private File file;
  private byte[] buffer = new byte[128 * 1024];

//  @Param({"10000", "100000", "1000000"})
  @Param({"10000"})
  private int numWrites;

  @Param({"0", "1", "10"})
  private int delayMs;

  @Setup
  public void setup() throws IOException {
    file = File.createTempFile("async-write-benchmark", ".tmp");
    System.err.println("Writing to " + file);
  }

  private class DelayingOutputStream extends OutputStream {
    private final OutputStream delegate;

    private DelayingOutputStream(OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(int i) throws IOException {
      delegate.write(i);
    }

    @Override
    public void write(@NotNull byte[] b, int off, int len) throws IOException {
      if (delayMs > 0) {
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  @Benchmark
  public void asyncOutputStream() throws IOException {
    try (OutputStream bos = new BufferedOutputStream(Files.newOutputStream(file.toPath()));
         DelayingOutputStream dos = new DelayingOutputStream(bos);
        AsyncOutputStream os = new AsyncOutputStream(dos)) {
      for (int i = 0; i < numWrites; i++) {
        os.write(buffer);
      }
    }
  }

  @Benchmark
  public void syncOutputStream() throws IOException {
    try (OutputStream bos = new BufferedOutputStream(Files.newOutputStream(file.toPath()));
         DelayingOutputStream os = new DelayingOutputStream(bos)) {
      for (int i = 0; i < numWrites; i++) {
        os.write(buffer);
      }
    }
  }
}
