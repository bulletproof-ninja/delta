
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

  implicit object CollegeEventCodec
      extends EventCodec[CollegeEvent, Array[Byte]]
      with NoVersioning[CollegeEvent, Array[Byte]] {

    def getName(cls: EventClass): String = cls.getName

    def encode(evt: CollegeEvent): Array[Byte] = JavaSerializer.encode(evt)
    def decode(channel: String, name: String, data: Array[Byte]) = JavaSerializer.decode(data).asInstanceOf[CollegeEvent]
  }

}
