package kanaloa.handler

import kanaloa.handler.GeneralActorRefHandler.ResultChecker

import scala.reflect._

object ResultChecker {

  /**
   * Result checker based on type
   *
   * @tparam ExpectedResultT
   * @return
   */
  def expectType[ExpectedResultT: ClassTag]: ResultChecker[ExpectedResultT, String] = (re: Any) ⇒ re match {
    case t: ExpectedResultT if classTag[ExpectedResultT].runtimeClass.isInstance(t) ⇒ Right(t)
    case e ⇒ Left(Some(s"Unexpected message $e"))
  }

  /**
   * always return success no mater what
   */
  val complacent: ResultChecker[Any, Nothing] = Right(_)
}
