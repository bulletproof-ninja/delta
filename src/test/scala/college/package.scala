
import language.implicitConversions

import delta._
import scuff.JavaSerializer

package college {
  case class IntId[T](int: Int) {
    override val toString = s"$int"
  }

  trait CollegeEvent

}
package object college {

  implicit def intId(id: IntId[_]): Int = id.int

  object CollegeEventFormat
      extends EventFormat[CollegeEvent, Array[Byte]] {

    def getVersion(cls: EventClass) = NoVersion
    def getName(cls: EventClass): String = cls.getName

    def encode(evt: CollegeEvent): Array[Byte] = JavaSerializer.encode(evt)
    def decode(encoded: Encoded) =
      JavaSerializer.decode(encoded.data).asInstanceOf[CollegeEvent]
  }

}
