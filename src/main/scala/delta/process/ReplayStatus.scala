package delta.process

trait ReplayStatus {
  def name: String
  /** @return Current total processed transactions */
  def totalTransactions: Long
  /** @return Current active transactions */
  def activeTransactions: Int
}
