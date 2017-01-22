
import language.implicitConversions

import college.semester._
import college.student._
import delta._
import delta.util._
import delta.ddd.EntityRepository
import scuff.JavaSerializer
import java.io.ByteArrayOutputStream
import scala.concurrent.ExecutionContext

package object college {

  implicit def intId(id: IntId[_]): Int = id.int

  case class IntId[T](int: Int)

  type SemesterId = IntId[Semester]
  type StudentId = IntId[Student]

  trait CollegeEvent

  implicit object CollegeEventCodec
      extends EventCodec[CollegeEvent, Array[Byte]]
      with NoVersioning[CollegeEvent, Array[Byte]] {

    def name(cls: EventClass): String = cls.getName

    def encode(evt: CollegeEvent): Array[Byte] = JavaSerializer.encode(evt)
    def decode(name: String, data: Array[Byte]) = JavaSerializer.decode(data).asInstanceOf[CollegeEvent]
  }

}
