package delta.ddd

/** Revision. */
sealed abstract class Revision {
  private[ddd] def validate(actual: Int): Unit =
    if (!matches(actual)) throw new Revision.MismatchException(value.get, actual)
  def matches(actual: Int): Boolean
  def value: Option[Int]
}
object Revision {

  def apply(expected: Option[Int]): Revision =
    expected match {
      case Some(expected) => apply(expected, false)
      case _ => Latest
    }
  def apply(expected: Int, exact: Boolean = false): Revision =
    if (expected < 0) Latest
    else if (exact) new Exactly(expected)
    else new Minimum(expected)

  /**
    * Exact revision.
    * For updates, this means no merging allowed.
    */
  case class Exactly(expected: Int) extends Revision {
    require(expected >= 0, s"Revision must be >= 0, was $expected")
    def value = Some(expected)
    def matches(actual: Int): Boolean = expected == actual
  }

  /**
    * Minimum revision.
    * For updates, if possible, merge if higher.
    */
  case class Minimum(expected: Int) extends Revision {
    require(expected >= 0, s"Revision must be >= 0, was $expected")
    def value = Some(expected)
    def matches(actual: Int): Boolean = actual >= expected
  }
  /**
    * Latest revsion.
    * Revision not known, or is irrelevant, use latest.
    */
  case object Latest extends Revision {
    def value = None
    def matches(actual: Int): Boolean = true
  }

  /** Revision mismatch exception. */
  final case class MismatchException(expected: Int, actual: Int)
    extends RuntimeException(s"Expected $expected, was $actual")
    with scala.util.control.NoStackTrace

}
