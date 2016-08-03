package kanaloa.stress

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.language.postfixOps

class KanaloaSimulation extends OverflowSimulation("kanaloa")

/**
 * Simulation against plain backend without kanaloa
 */
class StraightSimulation extends OverflowSimulation("straight")

abstract class OverflowSimulation(path: String) extends Simulation {

  val Url = s"http://localhost:8081/$path/test-1"

  val httpConf = http.disableCaching

  val scn = scenario("stress-test").forever {
    group("kanaloa") {
      exec(
        http("flood")
          .get(Url)
          .check(status.is(200))
      )
    }
  }

  setUp(scn.inject(
    rampUsers(500) over (5 minutes) //mainly by throttle below
  )).throttle(
    reachRps(200) in (5.minutes),
    holdFor(3.minute)
  )
    .protocols(httpConf)
    .assertions(global.responseTime.percentile3.lessThan(5000)) //95% less than 5s

}
