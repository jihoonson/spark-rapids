package org.apache.spark.sql.rapids

import java.io.{BufferedOutputStream, FileOutputStream, OutputStream}
import java.io.File

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class AsyncOutputStreamSuite extends AnyFunSuite with BeforeAndAfterEach {

  test("write") {
    val file = File.createTempFile("async-write-test", "tmp")
    val fos = new BufferedOutputStream(new FileOutputStream(file))
    val os = new AsyncOutputStream(fos)

    val before = System.currentTimeMillis()
    val buf: Array[Byte] = new Array[Byte](128 * 1024)
    for (_ <- 0 until 100000) {
      os.write(buf)
    }
    os.close()
    val after = System.currentTimeMillis()
    println(s"Time taken: ${after - before} ms")
  }
}
