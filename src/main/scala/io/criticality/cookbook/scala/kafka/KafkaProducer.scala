package io.criticality.cookbook.scala.kafka

import java.util.{ Properties, UUID }
import java.io._
import kafka.message.DefaultCompressionCodec
import kafka.message.NoCompressionCodec
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.producer._
import kafka.message._
import java.util.Properties

/**
 * Created by eschmiegelow on 03/02/15.
 */
case class KafkaProducer(topic: String,
                         brokerList: String,
                         /**
                          * brokerList
                          * This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas).
                          * The socket connections for sending the actual data will be established based on the broker information returned in
                          * the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a
                          * subset of brokers.
                          */
                         clientId: String = UUID.randomUUID().toString,
                         /**
                          * clientId
                          * The client id is a user-specified string sent in each request to help trace calls. It should logically identify
                          * the application making the request.
                          */
                         synchronously: Boolean = true,
                         /**
                          * synchronously
                          * This parameter specifies whether the messages are sent asynchronously in a background thread.
                          * Valid values are false for asynchronous send and true for synchronous send. By setting the producer
                          * to async we allow batching together of requests (which is great for throughput) but open the possibility
                          * of a failure of the client machine dropping unsent data.
                          */
                         compress: Boolean = true,
                         /**
                          * compress
                          * This parameter allows you to specify the compression codec for all data generated by this producer.
                          * When set to true gzip is used.  To override and use snappy you need to implement that as the default
                          * codec for compression using SnappyCompressionCodec.codec instead of DefaultCompressionCodec.codec below.
                          */

                         batchSize: Integer = 200,
                         /**
                          * batchSize
                          * The number of messages to send in one batch when using async mode.
                          * The producer will wait until either this number of messages are ready
                          * to send or queue.buffer.max.ms is reached.
                          */
                         messageSendMaxRetries: Integer = 3,
                         /**
                          * messageSendMaxRetries
                          * This property will cause the producer to automatically retry a failed send request.
                          * This property specifies the number of retries when such failures occur. Note that
                          * setting a non-zero value here can lead to duplicates in the case of network errors
                          * that cause a message to be sent but the acknowledgement to be lost.
                          */
                         requestRequiredAcks: Integer = -1 /** requestRequiredAcks
                           *  0) which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7).
                           *     This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
                           *  1) which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides
                           *     better durability as the client waits until the server acknowledges the request as successful (only messages that were
                           *     written to the now-dead leader but not yet replicated will be lost).
                           * -1) which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option
                           *     provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
                           */ ) {

  def this(topic: String, brokerList: String, clientId: String) = this(topic, brokerList, clientId, true, true, 200, 3, -1)

  val props = new Properties()

  val codec = if (compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  props.put("compression.codec", codec.toString)
  props.put("producer.type", if (synchronously) "sync" else "async")
  props.put("metadata.broker.list", brokerList)
  props.put("batch.num.messages", batchSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("request.required.acks", requestRequiredAcks.toString)
  props.put("client.id", clientId.toString)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def kafkaMesssage(message: Array[Byte], partition: Array[Byte]): KeyedMessage[AnyRef, AnyRef] = {
    if (partition == null) {
      new KeyedMessage(topic, message)
    } else {
      new KeyedMessage(topic, partition, message)
    }
  }

  def send(message: String, partition: String = null): Unit = send(message.getBytes("UTF8"), if (partition == null) null else partition.getBytes("UTF8"))

  def send(message: Array[Byte], partition: Array[Byte]): Unit = {
    try {

      val messsage: KeyedMessage[AnyRef, AnyRef] = kafkaMesssage(message, partition)
      producer.send(messsage)
    } catch {
      case e: Exception =>
        e.printStackTrace
    }
  }
}
