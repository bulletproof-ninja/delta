package delta

import java.util.Arrays

final case class Snapshot[+S](
  state: S,
  revision: Revision,
  tick: Tick) {

  def map[That](f: S => That): Snapshot[That] =
    new Snapshot(f(state), revision, tick)

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
