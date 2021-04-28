package delta

import java.util.Arrays
import scala.reflect.ClassTag

final case class Snapshot[+S](
  state: S,
  revision: Revision,
  tick: Tick) {

  def map[That](f: S => That): Snapshot[That] =
    new Snapshot(f(state), revision, tick)

  def flatMap[That](f: S => Option[That]): Option[Snapshot[That]] =
    f(state).map(s => this.copy(state = s))

  def collect[That](pf: PartialFunction[S, That]): Option[Snapshot[That]] =
    if (pf isDefinedAt state) Some(this.copy(state = pf(state)))
    else None

  def collectAs[That: ClassTag]: Option[Snapshot[That]] =
    state match {
      case that: That => Some(this.copy(state = that))
      case _ => None
    }

  def transpose[That](implicit ev: S <:< Option[That]): Option[Snapshot[That]] =
    state.map(state => this.copy(state = state))

  override lazy val hashCode: Int = {
    this.state match {
      case bytes: Array[Byte] => Arrays.hashCode(bytes) * 31 + revision * 31 + tick.## * 31
      case _ => super.hashCode()
    }
  }

  def stateEquals(thatState: Any): Boolean =
    this.state.getClass == thatState.getClass && {
    this.state match {
      case bytes: Array[Byte] => Arrays.equals(bytes, thatState.asInstanceOf[Array[Byte]])
      case _ => this.state == thatState
    }
  }

  def stateEquals(that: Snapshot[_]): Boolean = stateEquals(that.state)

  override def equals(other: Any): Boolean = other match {
    case that: Snapshot[_] =>
      this.revision == that.revision &&
      this.tick == that.tick &&
      this.stateEquals(that)
    case _ => false
  }
}
