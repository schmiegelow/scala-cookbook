package io.criticality.cookbook.scala.metrics

import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.graphite.Graphite
import com.codahale.metrics.graphite.GraphiteReporter
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import com.codahale.metrics.MetricFilter

class MetricsSender(graphiteHost: String, metricsPath: String, interval: Int) {

  val registry = new MetricRegistry()
  // graphite with carbon aggregator
  val graphite = new Graphite(new InetSocketAddress(graphiteHost,
    2023))
  val reporter = GraphiteReporter.forRegistry(registry)
    .prefixedWith(metricsPath)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .filter(MetricFilter.ALL)
    .build(graphite);
  reporter.start(interval, TimeUnit.SECONDS);
  val sampleMeter = registry.meter(MetricRegistry.name("tuples", "received"))

  def tick() {
    sampleMeter.mark()
  }

}