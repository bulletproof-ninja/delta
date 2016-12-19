package ulysses.util

trait CatchUpConsumer[T] extends (T => Unit) {

  /** Last processed tick, if exists. */
  def lastProcessedTick: Option[Long]
  /**
   *  Process (catch-up on) historic transactions.
   */
  def apply(txn: T): Unit
  /**
   * When caught up on historic transactions,
   * a live consumer is requested.
   */
  def liveConsumer: T => Unit
}
