package delta

import concurrent._, duration._

import scala.util.{ Random => rand }
import scuff._

package object testing {

  def NoopMessageHub[ID, S]: MessageHub[ID, S] = new MessageHub[ID, S] {

    def publish(id: ID, msg: S): Unit = ()

    def subscribe(id: ID)(callback: S => Unit): Subscription =
      new Subscription { def cancel() = () }

  }

  object EvtFmt
      extends EventFormat[Event, Array[Byte]] {

    def getVersion(cls: EventClass) = NotVersioned
    def getName(cls: EventClass): String = cls.getName

    def encode(evt: Event): Array[Byte] =
      JavaSerializer.encode(evt)
    def decode(encoded: Encoded): Event =
      JavaSerializer.decode(encoded.data).asInstanceOf[Event]
  }

  val isDebug = java.lang.management.ManagementFactory
    .getRuntimeMXBean
    .getInputArguments
    .toString.contains("jdwp")

  val AwaitDuration = if (isDebug) 16.hours else 360.seconds

  implicit class F[T](private val f: Future[T]) extends AnyVal {
    def await = Await.result(f, AwaitDuration)
  }

  implicit def toFuture[T](t: T) = Future successful t

  def randomName: String = {
    val upper: Char = rand.nextBetween('A', 'Z' + 1)
    val lower = (1 to rand.nextBetween(2, 13)).map(_ => rand.nextBetween('a', 'z' + 1)).map(_.toChar)
    (upper +: lower).mkString
  }

}
