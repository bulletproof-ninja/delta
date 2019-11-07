package delta

import _root_.java.util.Optional

package object java {
  implicit def optLongConversion(jlopt: Optional[_root_.java.lang.Long]): Option[Long] =
    if (jlopt.isPresent) Some(jlopt.get)
    else None
}
