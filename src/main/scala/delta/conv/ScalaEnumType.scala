package delta.conv

trait ScalaEnumType[EV <: Enumeration#Value] extends StorageType[EV] {
  protected def enum: Enumeration
  protected val byName: Map[String, EV] = {
    enum.values.foldLeft(Map.empty[String, EV]) {
      case (map, value) =>
        map.updated(value.toString, value.asInstanceOf[EV])
    }
  }
  override def writeAs(value: EV): AnyRef = value.toString
}
