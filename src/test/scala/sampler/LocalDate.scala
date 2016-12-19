package sampler

final case class MyDate(year: Short, month: Byte, day: Byte) {
  require(1 <= month && month <= 12)
  require(1 <= day && day <= 31)
  override def toString = f"$year%04d-$month%02d-$day%02d"
}
