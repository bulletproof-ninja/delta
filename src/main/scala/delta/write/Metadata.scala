package delta.write

trait Metadata {
  def toMap: Map[String, String]
}

object Metadata {

  val empty: Metadata = apply(Map.empty[String, String])

  def apply(map: Map[String, String]): Metadata = new Metadata {
    def toMap = map
    override def toString() = map.iterator.map {
      case (key, value) => s"$key=$value"
    }.mkString("[", ", ", "]")
  }

  def apply(values: (String, String)*): Metadata = {
    if (values.isEmpty) empty
    else apply(values.toMap)
  }

}
