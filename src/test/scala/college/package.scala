
import language.implicitConversions

import college.semester._
import college.student._
import delta._
import scuff.JavaSerializer

package college {
  case class IntId[T](int: Int)

  trait CollegeEvent

}
package object college {

  implicit def intId(id: IntId[_]): Int = id.int

  type SemesterId = IntId[Semester]
  type StudentId = IntId[Student]

  implicit object CollegeEventCodec
      extends EventCodec[CollegeEvent, Array[Byte]]
      with NoVersioning[CollegeEvent, Array[Byte]] {

    def nameOf(cls: EventClass): String = cls.getName

    def encode(evt: CollegeEvent): Array[Byte] = JavaSerializer.encode(evt)
    def decode(name: String, data: Array[Byte]) = JavaSerializer.decode(data).asInstanceOf[CollegeEvent]
  }

}
