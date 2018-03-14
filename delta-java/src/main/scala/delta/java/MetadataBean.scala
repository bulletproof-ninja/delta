package delta.java

import java.lang.reflect.Method
import java.beans.Introspector

/**
 *  Convenience interface for converting
 *  bean properties to a Scala Map[String, String].
 */
trait MetadataBean {

  /** Convert bean properties to Scala `Map`. */
  def toMap(): collection.Map[String, String] = {
    MetadataBean.propMethods(this).mapValues { readMethod =>
      String valueOf readMethod.invoke(this)
    }
  }

}

private object MetadataBean {
  private def propMethods(bean: MetadataBean): Map[String, Method] =
    _propMethods.get(bean.getClass)

  private[this] val _propMethods = new ClassValue[Map[String, Method]] {
    def computeValue(cls: Class[_]) = {
      val bi = Introspector.getBeanInfo(cls)
      bi.getPropertyDescriptors.filter(_.getReadMethod != null).map { pd =>
        val method = pd.getReadMethod
        method.setAccessible(true)
        pd.getName -> method
      }.toMap
    }
  }
}
