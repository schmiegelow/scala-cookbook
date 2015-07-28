package io.criticality.cookbook.scala.kafka

import java.util.Properties

import kafka.consumer.KafkaStream
import org.junit.{AfterClass, BeforeClass, Test}

/**
 * Created by e.schmiegelow on 15/04/15.
 */
class KafkaTopicConsumerTest {

  val run = false

  val zkPort = EmbeddedUtilities.getAvailablePort
  val zkTest = new EmbeddedZookeeper(zkPort, 100)
  val kafkaTest = new EmbeddedKafkaCluster(s"localhost:$zkPort", new Properties(), List(9092))


  @BeforeClass
  def startupTestEnv() = {
    zkTest.startup
    kafkaTest.startup
  }

  @AfterClass
  def shutdownTestEnv() = {
    kafkaTest.shutdown
    zkTest.shutdown
  }

  def readTopic(stream : KafkaStream[Array[Byte], Array[Byte]]): Unit = {
    stream.iterator() foreach (msg => {
      println("received key %s and msg %s " format(new String(msg.key()), new String(msg.message())))
    })
  }

  @Test
  def testConsumption(): Unit = {
    val runner = new Runner
    if (run) {
    runner.start()
    Thread.sleep(10000)
    // shouldn't do this, but consumers can otherwise run forever.
    runner.stop()
    }
  }

  class Runner extends Thread {
    val consumer = KafkaTopicConsumer("records", 1, "test01", s"localhost:${zkPort}")

    override def run(): Unit = {
      consumer.read(readTopic)
    }
  }

}
