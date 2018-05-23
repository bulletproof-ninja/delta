package delta.java

import java.lang.reflect.Method
import java.beans.Introspector
import scuff.Proxylicious
import scala.reflect.ClassTag
import scala.reflect.NameTransformer
import java.util.Optional

/**
  *  Convenience interface for converting
  *  bean properties to a Scala Map[String, String].
  */
trait MetadataBean {

  /** Convert bean properties to Scala `Map`. */
  def toMap(): Map[String, String] = {
    MetadataBean.propMethods(this).flatMap {
      case (name, readMethod) =>
        if (readMethod.getReturnType == classOf[Option[_]]) {
          readMethod.invoke(this).asInstanceOf[Option[Any]].map(value => name -> String.valueOf(value))
        } else if (readMethod.getReturnType == classOf[Optional[_]]) {
          val optional = readMethod.invoke(this).asInstanceOf[Optional[Any]]
          if (optional.isPresent) Some(name -> String.valueOf(optional.get)) else None
        } else {
          Some(name -> String.valueOf(readMethod invoke this))
        }
    }
  }

}

private object MetadataBean {

  private def propMethods(bean: MetadataBean): Map[String, Method] =
    _propMethods.get(bean.getClass)

  private[this] val _propMethods = new ClassValue[Map[String, Method]] {
    def computeValue(cls: Class[_]) = {
      val bi = Introspector.getBeanInfo(cls, classOf[Object])
      bi.getPropertyDescriptors.filter(_.getReadMethod != null).map { pd =>
        val method = pd.getReadMethod
        method.setAccessible(true)
        pd.getName -> method
      }.toMap
    }
  }

  def fromMap[B <: MetadataBean](map: java.util.Map[String, String], cls: Class[_ <: B]): B = {
    import scuff.reflect._
    val p = new Proxylicious[B]()(ClassTag(cls))
    p.proxify {
      case (_, method, Array()) =>
        val key = NameTransformer decode method.getName
        if (method.getReturnType == classOf[Option[_]]) map.get(key) match {
          case null => None
          case value => DynamicConstructor(value)(ClassTag(method.getReturnType))
        }
        else map.get(key) match {
          case null => null
          case value => DynamicConstructor(value)(ClassTag(method.getReturnType)).orNull
        }
    }
  }
}
