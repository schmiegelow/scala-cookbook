package io.criticality.cookbook.scala.metrics

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}

object MetricsSender {

  val config = ConfigFactory.load()

  val registry = new MetricRegistry()

  val graphite = new Graphite(new InetSocketAddress(config.as[String]("graphite.host"), config.as[Int]("graphite.port")))
  val reporter = GraphiteReporter.forRegistry(registry)
    .prefixedWith(config.as[String]("graphite.prefix"))
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .filter(MetricFilter.ALL)
    .build(graphite)
  reporter.start(1, TimeUnit.MINUTES)

  /**
   * marks a meter for a given metric name
   *
   * @param meterName name of this meter
   */
  def markMeter(meterName: String) = {
    registry.meter(meterName).mark()
  }

  /**
   * increases a counter for a given metric name
   *
   * @param counterName naem of this counter
   * @param value
   */
  def markCounter(counterName: String, value: Long) = {
    registry.counter(counterName).inc()
  }

}
