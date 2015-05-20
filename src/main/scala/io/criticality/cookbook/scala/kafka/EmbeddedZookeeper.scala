package io.criticality.cookbook.scala.kafka

import org.apache.zookeeper.server.NIOServerCnxnFactory
import org.apache.zookeeper.server.ServerCnxnFactory
import org.apache.zookeeper.server.ZooKeeperServer
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.InetSocketAddress

class EmbeddedZookeeper(var port: Int, var tickTime: Int) {
  private var factory: ServerCnxnFactory = null
  private var snapshotDir: File = null
  private var logDir: File = null

  
  def resolvePort(port: Int): Int = {
    if (port == -1) {
      EmbeddedUtilities.getAvailablePort
    }
    port
  }

  @throws(classOf[IOException])
  def startup {
    if (this.port == -1) {
      this.port = EmbeddedUtilities.getAvailablePort
    }
    this.factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress("localhost", port), 1024)
    this.snapshotDir = EmbeddedUtilities.constructTempDir("embeeded-zk/snapshot")
    this.logDir = EmbeddedUtilities.constructTempDir("embeeded-zk/log")
    try {
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime))
    }
    catch {
      case e: InterruptedException => {
        throw new IOException(e)
      }
    }
  }

  def shutdown {
    factory.shutdown
    try {
      EmbeddedUtilities.deleteFile(snapshotDir)
    }
    catch {
      case e: FileNotFoundException => {
      }
    }
    try {
      EmbeddedUtilities.deleteFile(logDir)
    }
    catch {
      case e: FileNotFoundException => {
      }
    }
  }

  def getConnection: String = {
    "localhost:" + port
  }

  def setPort(port: Int) {
    this.port = port
  }

  def setTickTime(tickTime: Int) {
    this.tickTime = tickTime
  }

  def getPort: Int = {
    port
  }

  def getTickTime: Int = {
    tickTime
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder("EmbeddedZookeeper{")
    sb.append("connection=").append(getConnection)
    sb.append('}')
    sb.toString
  }
}