package io.criticality.cookbook.scala.kafka

import kafka.consumer.KafkaStream
import org.junit.Test

/**
 * Created by e.schmiegelow on 15/04/15.
 */
class KafkaTopicConsumerTest {

  val run = false

  val consumer = KafkaTopicConsumer("records", 1, "test01", "localhost:2181")

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
    override def run(): Unit = {
      consumer.read(readTopic)
    }
  }

}
