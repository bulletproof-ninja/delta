package ulysses.conv

trait JavaEnumColumn[T <: java.lang.Enum[T]] extends ColumnType[T] {
  protected val byName =
    jvmType.getEnumConstants.foldLeft(Map.empty[String, T]) {
      case (map, value) =>
        val t = value.asInstanceOf[T]
        map.updated(t.name, t)
    }
  override def writeAs(value: T) = value.name
}
