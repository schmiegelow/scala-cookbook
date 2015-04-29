package io.criticality.cookbook.scala.flume

import org.slf4j.LoggerFactory
import org.apache.flume.EventDeliveryException
import org.apache.flume.api.RpcClientFactory
import org.apache.flume.Event

/**
 * Created by e.schmiegelow on 28/02/15.
 */
case class FlumeClient(hostname : String, port: Int) {

  val LOG = LoggerFactory.getLogger(classOf[FlumeClient])

  lazy val client = RpcClientFactory.getDefaultInstance(hostname, port)

  def sendEvent(event: Event) {

    // Send the event
    try {

      client.append(event);
    } catch {
      case e: EventDeliveryException => {
        client.close()
        LOG.error(e.getMessage, e)
      }
    }
  }

  def sendEvents(eventList: java.util.List[Event]) {

    try {
      client.appendBatch(eventList)
    } catch {
      case e: EventDeliveryException => {
        LOG.warn("no flume agent available - waiting for 10 seconds")
        try {
          Thread.sleep(10000)
        } catch {
          case e1: InterruptedException => {
            LOG.error(e1.getMessage, e1)
          }
        }
      }
    }
  }

  def cleanUp() {
    // Close the RPC connection
    client.close()
  }

}
