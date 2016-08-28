package ulysses.conv

import scala.reflect.classTag
import scala.reflect.runtime.ReflectionUtils.staticSingletonInstance

trait ScalaEnumConverter[E <: Enumeration] extends TypeConverter[E#Value] {
  protected def enumType: Class[E]
  protected lazy val byName = {
    val enum = staticSingletonInstance(enumType).asInstanceOf[E]
    enum.values.foldLeft(Map.empty[String, E#Value]) {
      case (map, value) =>
        map.updated(value.toString, value)
    }
  }
  override def writeAs(value: E#Value) = value.toString
}
