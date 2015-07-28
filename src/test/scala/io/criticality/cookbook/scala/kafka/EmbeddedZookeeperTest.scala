package io.criticality.cookbook.scala.kafka

import org.junit.Test
import org.junit.Assert._

/**
 * Created by e.schmiegelow on 30/06/15.
 */
class EmbeddedZookeeperTest {

  @Test
  @throws(classOf[Exception])
  def testStartup(): Unit = {
    val zkTest = new EmbeddedZookeeper(EmbeddedUtilities.getAvailablePort, 100)
    zkTest.startup
    println(s"running on ${zkTest.getPort}")
    zkTest.shutdown
  }
}