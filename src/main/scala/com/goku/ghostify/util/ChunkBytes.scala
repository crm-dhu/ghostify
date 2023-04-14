package com.goku.ghostify.util

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream, IOException}
import java.nio.file.{Files, Path}

import scala.collection.mutable.ArrayBuffer

object ChunkBytes {
  def readFileInByteChunks(inputPath: Path, BufferSize: Int = 1024 * 1024): Array[Array[Byte]] = {

    val fis = new FileInputStream(inputPath.toString)
    val sbc = Files.newByteChannel(inputPath).size()
    val MAX_FILE_SIZE = Integer.MAX_VALUE - 8

    if (sbc < MAX_FILE_SIZE) {
      Array(Files.readAllBytes(inputPath))
    } else {
      val varBytesBuffer = new ArrayBuffer[Array[Byte]]()
      var read = 0
      do {
        var chunkBuffer = new Array[Byte](BufferSize)
        read = fis.read(chunkBuffer, 0, BufferSize)
        varBytesBuffer append chunkBuffer
        chunkBuffer = null
      } while (read > -1)

      fis.close()
      varBytesBuffer.toArray
    }

  }

  def writeByteChunksInFile(outputPath: Path, chunkBytes: Array[Array[Byte]]): Unit = {

    try {
      val out = new BufferedOutputStream(new FileOutputStream(outputPath.toString))
      var count = 0

      while (count < chunkBytes.length) {
        val bytes = chunkBytes(count)
        out.write(bytes, 0, bytes.length)
        count += 1
      }
      out.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
