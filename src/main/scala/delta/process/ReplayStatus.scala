package delta.process

/**
  * Active replay status, for progress monitoring.
  */
trait ReplayStatus {
  def name: String
  /** @return Current total processed transactions */
  def totalTransactions: Long
  /** @return Current active transactions */
  def activeTransactions: Int
  /** @return Number of errors */
  def numErrors: Int
}
