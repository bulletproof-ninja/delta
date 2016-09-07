package ulysses.test_repo

final case class LocalDate(year: Short, month: Byte, day: Byte) {
  require(1 <= month && month <= 12)
  require(1 <= day && day <= 31)
  override def toString = s"$year-$month-$day"
}
