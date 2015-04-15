package io.criticality.app

import io.criticality.cookbook.scala.kafka.KafkaTopicConsumer
import kafka.consumer.KafkaStream
import org.joda.time.DateTime
import org.slf4j.LoggerFactory


/**
 * Created by e.schmiegelow on 15/04/15.
 */
class KafkaClientRunner(consumer: KafkaTopicConsumer) extends Thread {

  var LOG = LoggerFactory.getLogger(classOf[KafkaClientRunner])

  var count = 0

  def readTopic(stream : KafkaStream[Array[Byte], Array[Byte]]) = {
    stream.iterator() foreach (msg => {
      LOG.debug("received key %s and msg %s " format(String.valueOf(msg.key()), String.valueOf(msg.message())))
      count = count + 1
    })
  }

  override def run(): Unit = {
    consumer.read(readTopic)
  }

}

object KafkaClientRunner extends App {

  val topic : String = if (args.length > 0) args(0) else "records"
  val partitions = (if (args.length > 1) args(1) else "1").toInt
  val groupId = if (args.length > 2) args(2) else "test01"
  val zkHosts = if (args.length > 3) args(3) else "localhost:2181"

  val consumer = KafkaTopicConsumer(topic, partitions, groupId, zkHosts)
  var runnerList = List[KafkaClientRunner]()

  for (i <- 0 to partitions) {
    val runner = new KafkaClientRunner(consumer)
    runnerList = runner :: runnerList
    runner.start()
  }

  var currentCount = 0
  while (true) {
    val lastCount = currentCount
    runnerList foreach( f => {
      // accumulate
      currentCount = currentCount + f.count
      // reset current count
      f.count = 0})  

    println("%s --- Messages Consumed --- %s / %s" format(DateTime.now, (currentCount - lastCount), currentCount))
    Thread.sleep(1000)
  }

}