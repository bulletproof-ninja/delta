package delta.util

trait TwoPhaseProcessor[T] {

  /** Last processed tick, if exists. */
  def lastProcessedTick: Option[Long]
  /**
    *  Process historic transactions.
    */
  def historic: T => Unit
  /**
    * When caught up on historic transactions,
    * a live consumer is requested.
    */
  def live: T => Unit
}
