package kanaloa.queue

import akka.actor.PoisonPill
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.ApiProtocol.WorkFailed
import kanaloa.queue.Queue.{NoWorkLeft, Unregistered}
import kanaloa.queue.Worker.Retire

class WorkerUnregisteringIdleSpec extends WorkerSpec {

  final def withUnregisteringIdleWorker(test: (TestActorRef[Worker[Any]], TestProbe, TestProbe) ⇒ Any) {
    withIdleWorker() { (worker, queueProbe, routeeProbe, _) ⇒
      worker ! Retire //this puts the worker into the unregistering state
      test(worker, queueProbe, routeeProbe)
    }
  }

  "An Unregistering idle Worker" should {

    "status is UnregisteringIdle" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }

    "reject Work" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      val w = Work("more work")
      worker ! w
      expectMsg(Rejected(w, "Retiring"))
    }

    "terminate when Unregister" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Unregistered
      expectTerminated(worker)
    }

    "terminate if Terminated(queue)" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      queueProbe.ref ! PoisonPill
      expectTerminated(worker)
    }
  }
}

class WorkerWaitingToTerminateSpec extends WorkerSpec {

  final def withTerminatingWorker(settings: WorkSettings = WorkSettings())(test: (TestActorRef[Worker[Any]], TestProbe, TestProbe, Work[Any]) ⇒ Any) {
    withWorkingWorker(settings) { (worker, queueProbe, routeeProbe, work, _) ⇒
      worker ! NoWorkLeft //this changes the Worker's state into 'WaitingToTerminate
      test(worker, queueProbe, routeeProbe, work)
    }
  }

  "A Terminating Worker" should {
    "status is WaitingToTerminate" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "reject Work" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      val w = Work("more work")
      worker ! w
      expectMsg(Rejected(w, "Retiring"))
    }

    "terminate if the Routee dies" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.ref ! PoisonPill
      expectMsgType[WorkFailed]
      expectTerminated(worker)
    }

    "terminate when the Work completes" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.reply(Result("finished!"))
      expectMsg("finished!")
      expectTerminated(worker)
    }

    "terminate if the Work fails" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.reply(Fail("fail"))
      expectMsgType[WorkFailed]
      expectTerminated(worker)
    }
  }
}
