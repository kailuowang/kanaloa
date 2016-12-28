package kanaloa

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{Actor, ActorRef, Terminated}
import kanaloa.WorkerPoolSampler._
import kanaloa.Sampler.{Sample, AddSample}
import kanaloa.Types.{QueueLength, Speed}
import kanaloa.metrics.Metric._
import kanaloa.metrics.{Metric, MetricsCollector}
import kanaloa.queue.Queue
import kanaloa.util.Java8TimeExtensions._

import scala.concurrent.duration._

/**
 *  Mixed-in with [[MetricsCollector]] to which all [[Metric]] are sent to.
 *  Behind the scene it also collects performance [[WorkerPoolSample]] from [[WorkCompleted]] and [[WorkFailed]]
 *  when the system is in fullyUtilized state, namely when number
 *  of idle workers is less than [[kanaloa.Sampler.SamplerSettings]]
 *  It internally publishes these [[WorkerPoolSample]]s as well as [[PartialUtilization]] data
 *  which are only for internal tuning purpose, and should not be
 *  confused with the [[Metric]] used for realtime monitoring.
 *  It can be subscribed using [[kanaloa.Sampler.Subscribe]] message.
 *  It publishes [[WorkerPoolSample]]s and [[PartialUtilization]] number to subscribers.
 *
 */
private[kanaloa] trait WorkerPoolSampler extends Sampler {
  mc: MetricsCollector ⇒ //todo: it's using cake pattern to mixin with MetricsCollector mainly due to performance reason, there might be ways to achieve more decoupled ways without hurting performance

  import settings._

  def receive = partialUtilized(0)

  private def publishUtilization(idle: Int, poolSize: Int): Unit = {
    val utilization = poolSize - idle //todo: no longer valid, need to track separately
    publish(PartialUtilization(utilization))
    report(PoolUtilized(utilization))
  }

  private def reportQueueLength(queueLength: QueueLength): Unit =
    report(WorkQueueLength(queueLength.value))

  private def fullyUtilized(s: QueueStatus): Receive = handleSubscriptions orElse {
    case Queue.DispatchReport(s, _) ⇒ //todo: temporary mid step in refactor
      self ! s
    case Queue.Status(idle, workLeft, isFullyUtilized) ⇒
      reportQueueLength(workLeft)
      if (!isFullyUtilized) {
        val (rpt, _) = tryComplete(s)
        rpt foreach publish
        publishUtilization(idle, s.poolSize)
        context become partialUtilized(s.poolSize)
      } else
        context become fullyUtilized(s.copy(queueLength = workLeft))

    case metric: Metric ⇒
      handle(metric) {
        case WorkCompleted(processTime) ⇒
          val newWorkDone = s.workDone + 1
          val newAvgProcessTime = s.avgProcessTime.fold(processTime)(avg ⇒ ((avg * s.workDone.toDouble + processTime) / newWorkDone.toDouble))
          context become fullyUtilized(
            s.copy(
              workDone = newWorkDone,
              avgProcessTime = Some(newAvgProcessTime)
            )
          )
        case WorkFailed ⇒
          context become fullyUtilized(s.copy(workDone = s.workDone + 1))

        case PoolSize(size) ⇒
          val sizeChanged = s.poolSize != size
          if (sizeChanged) {
            val (r, _) = tryComplete(s)
            r foreach publish
            context become fullyUtilized(
              QueueStatus(poolSize = size, queueLength = s.queueLength)
            )
          }
      }

    case AddSample ⇒
      val (rep, status) = tryComplete(s)
      rep foreach publish
      context become fullyUtilized(status)
      report(PoolUtilized(s.poolSize)) //take the chance to report utilization to reporter
  }

  private def partialUtilized(poolSize: Int): Receive = handleSubscriptions orElse {
    case Queue.DispatchReport(s, _) ⇒ //todo: temporary mid step in refactor
      self ! s
    case Queue.Status(idle, queueLength, isFullyUtilized) ⇒
      if (isFullyUtilized) {
        context become fullyUtilized(
          QueueStatus(poolSize = poolSize, queueLength = queueLength)
        )
      } else
        publishUtilization(idle, poolSize)
      reportQueueLength(queueLength)

    case metric: Metric ⇒
      handle(metric) {
        case PoolSize(s) ⇒
          context become partialUtilized(s)
      }
    case AddSample ⇒ //no sample is produced in the partial utilized state
  }

  /**
   *
   * @param so o l
   * @return a reset status if completes, the original status if not.
   */
  private def tryComplete(status: QueueStatus): (Option[Report], QueueStatus) = {
    val sample = status.toSample(minSampleDuration)

    val newStatus = if (sample.fold(false)(_.workDone > 0))
      status.copy(workDone = 0, start = Time.now, avgProcessTime = None) //if sample is valid and there is work done restart the counter
    else status

    (sample, newStatus)
  }

}

private[kanaloa] object WorkerPoolSampler {

  private case class QueueStatus(
    queueLength:    QueueLength,
    workDone:       Int              = 0,
    start:          Time             = Time.now,
    poolSize:       Int              = 0,
    avgProcessTime: Option[Duration] = None
  ) {

    def toSample(minSampleDuration: Duration): Option[WorkerPoolSample] = {
      if (duration >= minSampleDuration) Some(WorkerPoolSample(
        workDone = workDone,
        start = start,
        end = Time.now,
        poolSize = poolSize,
        queueLength = queueLength,
        avgProcessTime = avgProcessTime
      ))
      else
        None
    }

    def duration = start.until(Time.now)

  }

  sealed trait Report extends Sample

  case class WorkerPoolSample(
    workDone:       Int,
    start:          Time,
    end:            Time,
    poolSize:       Int,
    queueLength:    QueueLength,
    avgProcessTime: Option[Duration]
  ) extends Report {
    /**
     * Work done per milliseconds
     */
    lazy val speed: Speed = Speed(workDone.toDouble * 1000 / start.until(end).toMicros.toDouble)
  }

  /**
   * Number of utilized the workers in the worker when not all workers in the pool are busy
   *
   * @param numOfBusyWorkers
   */
  //todo: move this to queue sampler
  case class PartialUtilization(numOfBusyWorkers: Int) extends Report

}
