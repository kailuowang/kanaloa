package kanaloa.metrics

import akka.actor._
import kanaloa.WorkerPoolSampler
import kanaloa.Sampler.SamplerSettings

/**
 *  A metrics collector to which all [[Metric]] are sent to.
 *  This can be mixed in to inject other metrics related behavior, see [[WorkerPoolSampler]]
 */
trait MetricsCollector extends Actor {

  def reporter: Option[Reporter]

  protected def handle(metric: Metric)(pf: PartialFunction[Metric, Unit]): Unit = {
    report(metric)
    pf.applyOrElse(metric, (_: Metric) ⇒ ())
  }

  protected def report(metric: Metric): Unit = {
    if (!reporter.isEmpty) reporter.get.report(metric) //better performance than Option.foreach
  }

}

private[kanaloa] object MetricsCollector {

  class MetricsCollectorImpl(
    val reporter: Option[Reporter],
    val settings: SamplerSettings
  ) extends MetricsCollector with WorkerPoolSampler

  def props(
    reporter: Option[Reporter],
    settings: SamplerSettings  = SamplerSettings()
  ): Props = Props(new MetricsCollectorImpl(reporter, settings))
}
