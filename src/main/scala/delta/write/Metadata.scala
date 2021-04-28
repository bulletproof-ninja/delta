package delta.write

trait Metadata {
  def toMap: Map[String, String]
}

object Metadata {

  final val Empty: Metadata = apply(Map.empty[String, String])

  def apply(map: Map[String, String]): Metadata =
    new Metadata {
      def toMap = map
      override def toString() = map.iterator.map {
        case (key, value) => s"$key=$value"
      }.mkString("[", ", ", "]")
    }

  def apply(values: (String, String)*): Metadata = {
    if (values.isEmpty) Empty
    else apply(values.toMap)
  }

}
