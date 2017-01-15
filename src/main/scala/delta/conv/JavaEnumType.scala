package delta.conv

trait JavaEnumType[E <: java.lang.Enum[E]] extends StorageType[E] {
  protected val byName =
    jvmType.getEnumConstants.foldLeft(Map.empty[String, E]) {
      case (map, value) =>
        val t = value.asInstanceOf[E]
        map.updated(t.name, t)
    }
  override def writeAs(value: E) = value.name
}
