package delta

import java.util.Arrays

case class Snapshot[+Content](
  content: Content,
  revision: Int,
  tick: Long) {

  def map[That](f: Content => That): Snapshot[That] =
    new Snapshot(f(content), revision, tick)

  def transpose[That](implicit ev: Content <:< Option[That]): Option[Snapshot[That]] =
    content.map(content => this.copy(content = content))

  override lazy val hashCode: Int = {
    this.content match {
      case bytes: Array[Byte] => Arrays.hashCode(bytes) * 31 + revision * 31 + tick.## * 31
      case _ => super.hashCode()
    }
  }

  def contentEquals(thatContent: Any): Boolean =
    this.content.getClass == thatContent.getClass && {
    this.content match {
      case bytes: Array[Byte] => Arrays.equals(bytes, thatContent.asInstanceOf[Array[Byte]])
      case _ => this.content == thatContent
    }
  }

  def contentEquals(that: Snapshot[_]): Boolean = contentEquals(that.content)

  override def equals(other: Any): Boolean = other match {
    case that: Snapshot[_] =>
      (this.revision == that.revision) &&
        (this.tick == that.tick) &&
        this.contentEquals(that)
    case _ => false
  }
}
