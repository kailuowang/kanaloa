package kanaloa.dispatcher

import akka.actor._
import kanaloa.dispatcher.ActorBackend.BackendAdaptors.UnexpectedRequest

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect._

trait ActorBackend {
  def apply(f: ActorRefFactory): Future[ActorRef]
}

object ActorBackend {

  def apply(f: ActorRefFactory ⇒ ActorRef): ActorBackend = new ActorBackend {
    def apply(factory: ActorRefFactory): Future[ActorRef] = Future.successful(f(factory))
  }

  trait BackendAdaptor[T] {
    def apply(t: T): ActorBackend
  }

  object BackendAdaptor extends BackendAdaptors

  trait BackendAdaptors {

    // helper to create an adapator from a function
    def apply[T](f: T ⇒ ActorBackend): BackendAdaptor[T] = new BackendAdaptor[T] {
      def apply(t: T): ActorBackend = f(t)
    }

    implicit def backendBackendAdaptor[T <: ActorBackend] = apply[T](identity)

    // accepting subtypes of ActorRef to also support TestActorRef
    implicit def actorRefBackend[T <: ActorRef]: BackendAdaptor[T] = apply[T](ref ⇒ ActorBackend(_ ⇒ ref))

    implicit val propsBackend = apply[Props](props ⇒ ActorBackend(_.actorOf(props)))

    implicit def functionBackend[ReqT: ClassTag, ResT]: BackendAdaptor[ReqT ⇒ Future[ResT]] =
      apply[ReqT ⇒ Future[ResT]] { f ⇒
        ActorBackend(_.actorOf(Props(new SimpleFunctionDelegatee[ReqT, ResT](f)).withDeploy(Deploy.local)))
      }

    private class SimpleFunctionDelegatee[ReqT: ClassTag, ResT](f: ReqT ⇒ Future[ResT]) extends Actor {
      import context.dispatcher
      def receive: Receive = {
        case t: ReqT if classTag[ReqT].runtimeClass.isInstance(t) ⇒
          val replyTo = sender
          f(t).recover {
            case e: Throwable ⇒ e
          }.foreach(replyTo ! _)

        case m ⇒ sender ! UnexpectedRequest(m)
      }
    }

  }

  object BackendAdaptors {
    case class UnexpectedRequest(request: Any) extends Exception
  }

  implicit def backendAdaptorToBackend[T](t: T)(implicit adaptor: BackendAdaptor[T]): ActorBackend = adaptor(t)

}
