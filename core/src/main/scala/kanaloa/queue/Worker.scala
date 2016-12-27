package kanaloa.queue

import java.time.LocalDateTime

import akka.actor._
import kanaloa.ApiProtocol.{QueryStatus, WorkFailed, WorkTimedOut}
import kanaloa.handler._

import kanaloa.metrics.Metric
import kanaloa.queue.Queue.{NoWorkLeft, RequestWork, Unregister, Unregistered}
import kanaloa.queue.Worker._
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._

private[queue] class Worker[T](
  queue:                  QueueRef,
  metricsCollector:       ActorRef,
  handler:                Handler[T],
  circuitBreakerSettings: Option[CircuitBreakerSettings]

) extends Actor with ActorLogging with MessageScheduler {

  //if a WorkSender fails, Let the DeathWatch handle it
  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  var timeoutCount: Int = 0
  var delayBeforeNextWork: Option[FiniteDuration] = None

  var workCounter: Long = 0

  private val circuitBreaker: Option[CircuitBreaker] = circuitBreakerSettings.map(new CircuitBreaker(_))

  def receive = waitingForWork

  override def preStart(): Unit = {
    super.preStart()
    context watch queue
    queue ! RequestWork(self)
  }

  def finish(reason: String): Unit = {
    log.debug(s"Stopping due to: $reason")
    context stop self
  }

  val waitingForWork: Receive = {
    case qs: QueryStatus                  ⇒ qs reply Idle

    case work: Work[T]                    ⇒ sendWorkToHandler(work, 0)

    //If there is no work left, or if the Queue dies, the Worker stops as well
    case NoWorkLeft | Terminated(`queue`) ⇒ finish("Queue reports no Work left")

    //if the Routee dies or the Worker is told to Retire, it needs to Unregister from the Queue before terminating
    case Worker.Retire                    ⇒ becomeUnregisteringIdle()
  }

  def working(outstanding: Outstanding): Receive = handleRouteeResponse(outstanding) orElse {
    case qs: QueryStatus                  ⇒ qs reply Working

    //we are done with this Work, ask for more and wait for it
    case WorkFinished                     ⇒ askMoreWork()
    case w: Work[_]                       ⇒ sender() ! Rejected(w, "Busy")

    //if there is no work left, or if the Queue dies, the Actor must wait for the Work to finish before terminating
    case Terminated(`queue`) | NoWorkLeft ⇒ context become waitingToTerminate(outstanding)

    case Worker.Retire ⇒
      context become waitingToTerminate(outstanding) //when busy no need to unregister, because it's not in the queue's list.
  }

  //This state waits for Work to complete, and then stops the Actor
  def waitingToTerminate(outstanding: Outstanding): Receive = handleRouteeResponse(outstanding) orElse {
    case qs: QueryStatus                           ⇒ qs reply WaitingToTerminate

    //ignore these, since all we care about is the Work completing one way or another
    case Retire | Terminated(`queue`) | NoWorkLeft ⇒

    case w: Work[_]                                ⇒ sender() ! Rejected(w, "Retiring") //safety first

    case WorkFinished ⇒
      finish(s"Finished work, time to retire") //work is done, terminate
  }

  //in this state, we have told the Queue to Unregister this Worker, so we are waiting for an acknowledgement
  val unregisteringIdle: Receive = {
    case qs: QueryStatus     ⇒ qs reply UnregisteringIdle

    //ignore these
    case Retire | NoWorkLeft ⇒

    case w: Work[T]          ⇒ sender ! Rejected(w, "Retiring") //safety first

    //Either we Unregistered successfully, or the Queue died.  terminate
    case Unregistered ⇒
      finish(s"Idle worker finished unregistration.")

    case Terminated(`queue`) ⇒
      finish("Idle working detected Queue terminated")
  }

  def becomeUnregisteringIdle(): Unit = {
    queue ! Unregister(self)
    context become unregisteringIdle
  }

  //onRouteeFailure is what gets called if while waiting for a Routee response, the Routee dies.
  def handleRouteeResponse(outstanding: Outstanding): Receive = {

    case wr: WorkResult[handler.Error, handler.Resp] if wr.workId == outstanding.workId ⇒ {

      wr.result.instruction.foreach { //todo: these instructions should happen at the processor level once it's processor per handler
        case Terminate         ⇒ self ! Retire
        case Hold(duration)    ⇒ delayBeforeNextWork = Some(duration)
        case RetryIn(duration) ⇒ //todo: implement this retry
      }

      wr.result.reply match {
        case Right(res) ⇒
          outstanding.success(res)
          self ! WorkFinished

        case Left(e) ⇒ //todo: to be addressed by
          retryOrAbandon(outstanding, e)
      }

    }

    case WorkResult(wId, x) ⇒ //should never happen..right?
      log.error("Received a response for a request which has already been serviced")

    case HandlerTimeout ⇒
      log.warning(s"handler ${handler.name} timed out after ${outstanding.work.settings.timeout} work ${outstanding.work.messageToDelegatee} abandoned")
      outstanding.timeout()
      self ! WorkFinished

  }

  private def retryOrAbandon(outstanding: Outstanding, error: Any): Unit = {
    outstanding.cancel()
    val errorDesc = descriptionOf(error, outstanding.work.settings.lengthOfDisplayForMessage)
    if (outstanding.retried < outstanding.work.settings.retry) {
      log.info(s"Retry work $outstanding due to error $errorDesc")
      sendWorkToHandler(outstanding.work, outstanding.retried + 1)
    } else {
      def message = {
        val retryMessage = if (outstanding.retried > 0) s"after ${outstanding.retried + 1} try(s)" else ""
        s"Processing of '${outstanding.workDescription}' failed $retryMessage due to error $errorDesc"
      }
      log.warning(s"$message, work abandoned")
      outstanding.fail(WorkFailed(message + s" due to $errorDesc"))
      self ! WorkFinished
    }
  }

  private def sendWorkToHandler(work: Work[T], retried: Int): Unit = {
    workCounter += 1 //do we increase this on a retry?
    val newWorkId = workCounter

    val handling = handler.handle(work.messageToDelegatee)
    import context.dispatcher
    handling.result.foreach { r ⇒
      self ! WorkResult(newWorkId, r)
    }
    val timeout = delayedMsg(work.settings.timeout, HandlerTimeout)
    val out = Outstanding(work, newWorkId, timeout, handling, retried)
    context become working(out)

  }

  private def askMoreWork(): Unit = {
    maybeDelayedMsg(delayBeforeNextWork, RequestWork(self), queue)
    context become waitingForWork
  }

  private class CircuitBreaker(settings: CircuitBreakerSettings) {
    def resetTimeoutCount(): Unit = {
      timeoutCount = 0
      delayBeforeNextWork = None
    }

    def incrementTimeoutCount(): Unit = {
      timeoutCount = timeoutCount + 1
      if (timeoutCount >= settings.timeoutCountThreshold) {
        delayBeforeNextWork = Some(settings.openDurationBase * timeoutCount.toLong)
        if (timeoutCount == settings.timeoutCountThreshold) //just crossed the threshold
          metricsCollector ! Metric.CircuitBreakerOpened
      }

    }
  }

  protected case class Outstanding(
    work:          Work[T],
    workId:        Long,
    timeoutHandle: akka.actor.Cancellable,
    handling:      Handling[_, _],
    retried:       Int                    = 0,
    startAt:       LocalDateTime          = LocalDateTime.now
  ) {
    def success(result: Any): Unit = {
      done(result)
      val duration = startAt.until(LocalDateTime.now)
      circuitBreaker.foreach(_.resetTimeoutCount())
      metricsCollector ! Metric.WorkCompleted(duration)
    }

    def fail(result: Any): Unit = {
      done(result)
      circuitBreaker.foreach(_.resetTimeoutCount())
      metricsCollector ! Metric.WorkFailed
    }

    def timeout(): Unit = {
      done(WorkTimedOut(s"Delegatee didn't respond within ${work.settings.timeout}"))
      handling.cancellable.foreach(_.cancel())
      circuitBreaker.foreach(_.incrementTimeoutCount())
      metricsCollector ! Metric.WorkTimedOut
    }

    protected def done(result: Any): Unit = {
      cancel()
      reportResult(result)
    }

    def cancel(): Unit = {
      timeoutHandle.cancel()
      handling.cancellable.foreach(_.cancel())
    }

    lazy val workDescription = descriptionOf(work.messageToDelegatee, work.settings.lengthOfDisplayForMessage)

    def reportResult(result: Any): Unit = work.replyTo.foreach(_ ! result)

  }
}

private[queue] object Worker {
  case class WorkResult[TResp, TError](workId: Long, result: Result[TResp, TError])

  private case object HandlerTimeout
  case object Retire

  sealed trait WorkerStatus
  case object UnregisteringIdle extends WorkerStatus
  case object Idle extends WorkerStatus
  case object Working extends WorkerStatus
  case object WaitingToTerminate extends WorkerStatus

  private[queue] case object WorkFinished

  def default[T](
    queue:                  QueueRef,
    handler:                Handler[T],
    metricsCollector:       ActorRef,
    circuitBreakerSettings: Option[CircuitBreakerSettings] = None
  ): Props = {
    Props(new Worker[T](queue, metricsCollector, handler, circuitBreakerSettings)).withDeploy(Deploy.local)
  }

  private[queue] def descriptionOf(any: Any, maxLength: Int): String = {
    val msgString = any.toString
    if (msgString.length < maxLength)
      msgString
    else {
      val firstWhitespaceAfterMax = msgString.indexWhere(c ⇒ (c.isWhitespace || !c.isLetterOrDigit), maxLength)
      val truncateAt = if (firstWhitespaceAfterMax < 0) maxLength else firstWhitespaceAfterMax
      msgString.take(truncateAt).trim + "..."
    }
  }
}
