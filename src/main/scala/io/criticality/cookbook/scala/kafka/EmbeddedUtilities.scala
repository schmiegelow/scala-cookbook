package io.criticality.cookbook.scala.kafka

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.ServerSocket
import java.util.Random

object EmbeddedUtilities {
  private val RANDOM: Random = new Random

  def constructTempDir(dirPrefix: String): File = {
    val file: File = new File(System.getProperty("java.io.tmpdir"), dirPrefix + RANDOM.nextInt(10000000))
    if (!file.mkdirs) {
      throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath)
    }
    file.deleteOnExit
    return file
  }

  def getAvailablePort: Int = {
    try {
      val socket: ServerSocket = new ServerSocket(0)
      try {
        return socket.getLocalPort
      } finally {
        socket.close
      }
    }
    catch {
      case e: IOException => {
        throw new IllegalStateException("Cannot find available port: " + e.getMessage, e)
      }
    }
  }

  @throws(classOf[FileNotFoundException])
  def deleteFile(path: File): Boolean = {
    if (!path.exists) {
      throw new FileNotFoundException(path.getAbsolutePath)
    }
    var ret: Boolean = true
    if (path.isDirectory) {
      for (f <- path.listFiles) {
        ret = ret && deleteFile(f)
      }
    }
    return ret && path.delete
  }
}

