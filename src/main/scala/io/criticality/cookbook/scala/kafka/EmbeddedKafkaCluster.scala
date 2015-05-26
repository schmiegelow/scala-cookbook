package io.criticality.cookbook.scala.kafka

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.Time
import java.io.File
import java.io.FileNotFoundException
import java.util.Collections
import java.util.Properties

/**
 * Created by e.schmiegelow on 20/05/15.
 */
case class EmbeddedKafkaCluster(zkConnection: String, baseProperties: Properties, ports: List[Integer]) {

  val logDir: File = EmbeddedUtilities.constructTempDir("kafka-local")
  val properties: Properties = new Properties

  var brokers : List[KafkaServer] = List()
  var logDirs : List[File] = List()

  val brokerList : String = constructBrokerList(ports)


  def this(zkConnection: String, baseProperties: Properties) {
    this(zkConnection, baseProperties, List(-1))
  }


  private def resolvePorts(ports: List[Integer]): List[Integer] = {
    var resolvedPorts: List[Integer] = List[Integer]()
    for (port <- ports) {
      resolvedPorts = resolvePort(port) :: resolvedPorts
    }
    resolvedPorts
  }

  private def resolvePort(port: Int): Int = {
    if (port == -1) {
      return EmbeddedUtilities.getAvailablePort
    }
    port
  }

  private def constructBrokerList(ports: List[Integer]): String = {
    ports.map(port => { "localhost:%," format port}).dropRight(1)
  }

  def startup {
    {
      var i: Int = 0
      while (i < ports.size) {
        {
          val port: Integer = ports(i)
          properties.putAll(baseProperties)
          properties.setProperty("zookeeper.connect", zkConnection)
          properties.setProperty("broker.id", String.valueOf(i + 1))
          properties.setProperty("host.name", "localhost")
          properties.setProperty("port", Integer.toString(port))
          properties.setProperty("log.dir", logDir.getAbsolutePath)
          properties.setProperty("log.flush.interval.messages", String.valueOf(1))
          val broker: KafkaServer = startBroker(properties)
          brokers = broker :: brokers
          logDirs = logDir :: logDirs
          i += 1
        }

      }
    }
  }

  private def startBroker(props: Properties): KafkaServer = {
    val server: KafkaServer = new KafkaServer(new KafkaConfig(props), new SystemTime)
    server.startup
    server
  }

  def getClientProps: Properties = {
    val props: Properties = new Properties
    props.putAll(baseProperties)
    props.put("metadata.broker.list", brokerList)
    props.put("zookeeper.connect", zkConnection)
    props
  }


  def shutdown {
    for (broker <- brokers) {
      try {
        broker.shutdown
      }
      catch {
        case e: Exception => {
          e.printStackTrace
        }
      }
    }

    for (logDir <- logDirs) {
      try {
        EmbeddedUtilities.deleteFile(logDir)
      }
      catch {
        case e: FileNotFoundException => {
          e.printStackTrace
        }
      }
    }
  }

  override def toString: String = {
    "EmbeddedKafkaCluster{'%s'}" format brokerList
  }
}

class SystemTime extends Time {
  def milliseconds: Long = {
    return System.currentTimeMillis
  }

  def nanoseconds: Long = {
    return System.nanoTime
  }

  def sleep(ms: Long) {
    try {
      Thread.sleep(ms)
    }
    catch {
      case e: InterruptedException => {
      }
    }
  }
}