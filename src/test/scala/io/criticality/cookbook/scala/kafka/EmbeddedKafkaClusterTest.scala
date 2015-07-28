package io.criticality.cookbook.scala.kafka

import java.util.Properties

import org.junit.Test
import org.junit.Assert._

/**
 * Created by e.schmiegelow on 30/06/15.
 */
class EmbeddedKafkaClusterTest {
  @Test
  @throws(classOf[Exception])
  def testStartup: Unit = {
    val zkTest = new EmbeddedZookeeper(EmbeddedUtilities.getAvailablePort, 100)
    zkTest.startup
    println(s"running on ${zkTest.getPort}")
    val kafkaTest = new EmbeddedKafkaCluster(s"localhost:${zkTest.getPort}", new Properties(), List(9092))
    kafkaTest.startup
    kafkaTest.shutdown
    zkTest.shutdown
  }
}