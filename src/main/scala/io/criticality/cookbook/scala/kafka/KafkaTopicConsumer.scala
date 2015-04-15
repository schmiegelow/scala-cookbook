package io.criticality.cookbook.scala.kafka

import java.util.Properties

import kafka.consumer.ConsumerConfig
import kafka.consumer.KafkaStream;

/**
 * Created by e.schmiegelow on 15/04/15.
 */
case class KafkaTopicConsumer(topic : String, partitions : Int, groupId : String, zkHosts : String) {

  lazy val consumer = kafka.consumer.Consumer.create(props())
  val topicCountMap = Map(topic -> partitions)

  def props() : ConsumerConfig = {
    val props = new Properties();
    props.put("zookeeper.connect", zkHosts);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    new ConsumerConfig(props)
  }

  def read(consume: (KafkaStream[Array[Byte], Array[Byte]]) => Unit): Unit = {
    val kafkaStream = consumer.createMessageStreams(topicCountMap)
    kafkaStream(topic) foreach( stream => consume(stream))
  }

}
