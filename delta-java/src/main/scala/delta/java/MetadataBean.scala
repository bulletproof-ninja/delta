package delta.java

import java.beans.Introspector
import java.lang.reflect.Method
import java.util.Optional

import scala.reflect.ClassTag

import scuff.Proxylicious

/**
  *  Convenience interface for converting
  *  bean properties to a Scala Map[String, String].
  */
trait MetadataBean {

  /** Convert bean properties to Scala `Map`. */
  def toMap(): Map[String, String] = {
    MetadataBean.propMethods(this.getClass).flatMap {
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

  private[this] val getClassMethod = classOf[Object].getMethod("getClass")

  private def propMethods(beanClass: Class[_ <: MetadataBean]): Map[String, Method] =
    _propMethods.get(beanClass)._1
  private def methodProps(beanInterface: Class[_ <: MetadataBean]): Map[Method, String] =
    _propMethods.get(beanInterface)._2

  private[this] val _propMethods = new ClassValue[(Map[String, Method], Map[Method, String])] {
    def computeValue(cls: Class[_]) = {
      val bi = Introspector.getBeanInfo(cls)
      val propMethods = bi.getPropertyDescriptors
        .filter(_.getReadMethod != null)
        .filter(_.getReadMethod != getClassMethod)
        .map { pd =>
          val method = pd.getReadMethod
          method.setAccessible(true)
          pd.getName -> method
        }.toMap
      val methodProps = propMethods.map {
        case (prop, method) => method -> prop
      }
      propMethods -> methodProps
    }
  }

  def fromMap[B <: MetadataBean](map: java.util.Map[String, String], beanInterface: Class[_ <: B]): B = {
    import scuff.reflect._
    val p = new Proxylicious[B]()(ClassTag(beanInterface))
    val propNames = methodProps(beanInterface)
    p.proxify {
      case (_, method, _) =>
        val propName = propNames(method)
        map.get(propName) match {
          case null =>
            if (method.getReturnType == classOf[Option[_]]) None
            else if (method.getReturnType == classOf[Optional[_]]) Optional.empty
            else null
          case stringValue =>
            val typedValue = DynamicConstructor[Any](stringValue)(ClassTag(method.getReturnType))
            println(s"Raw value $stringValue was turned into $typedValue")
            if (method.getReturnType == classOf[Option[_]]) typedValue
            else if (method.getReturnType == classOf[Optional[_]]) Optional.ofNullable(typedValue.orNull)
            else typedValue.orNull
        }
    }
  }
}
