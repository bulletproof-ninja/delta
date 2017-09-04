package delta.ddd

/** Revision. */
sealed abstract class Revision {
  private[ddd] def validate(actual: Int): Unit
  def value: Option[Int]
}
object Revision {

  def apply(expected: Option[Int]): Revision =
    expected match {
      case Some(expected) => apply(expected, false)
      case _ => Latest
    }
  def apply(expected: Int, exact: Boolean = false): Revision =
    if (exact) new Exactly(expected)
    else new Minimum(expected)

  /**
    * Exact revision.
    * For updates, this means no merging allowed.
    */
  case class Exactly(expected: Int) extends Revision {
    def value = Some(expected)
    private[ddd] def validate(actual: Int): Unit = {
      if (expected != actual) {
        throw new MismatchException(expected, actual)
      }
    }
  }

  /**
    * Minimum revision.
    * For updates, if possible, merge if higher.
    */
  case class Minimum(expected: Int) extends Revision {
    def value = Some(expected)
    private[ddd] def validate(actual: Int): Unit = {
      if (expected > actual) {
        throw new MismatchException(expected, actual)
      }
    }
  }
  /**
    * Latest revsion.
    * Revision not known, or is irrelevant, use latest.
    */
  case object Latest extends Revision {
    def value = None
    private[ddd] def validate(actual: Int): Unit = ()
  }

  /** Revision mismatch exception. */
  final case class MismatchException(expected: Int, actual: Int)
    extends RuntimeException(s"Expected $expected, was $actual")
    with scala.util.control.NoStackTrace

}
