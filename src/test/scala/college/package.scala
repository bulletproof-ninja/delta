
import delta._

import scuff.JavaSerializer
import scuff.Timestamp

import scala.concurrent._
import delta.validation.ConsistencyValidation
import scala.util.Random

package college {

  case class IntId[T](int: Int = Random.nextInt()) {
    override val toString = s"$int"
  }

  trait CollegeEvent

  case class CollegeMD(time: Timestamp = new Timestamp)
  extends write.Metadata {
    def toMap = Map("timetamp" -> time.asMillis.toString)
  }

  class CollegeRepo[ID <: IntId[E], E, EVT <: CollegeEvent, S >: Null](
    entity: write.Entity[S, EVT] { type Id = ID; type Type = E })(
    eventStore: EventStore[Int, _ >: EVT])(
    implicit
    exeCtx: ExecutionContext)
  extends write.EntityRepository[Int, EVT, S, ID, E](entity)(eventStore)

}

package object college {

  import student._
  import semester._

  type StudentRepo = CollegeRepo[Student.Id, Student, StudentEvent, StudentState]
  type SemesterRepo = CollegeRepo[Semester.Id, Semester, SemesterEvent, SemesterState]

  type CollegeEventStore =
    EventSource[Int, CollegeEvent] with
    ConsistencyValidation[Int, CollegeEvent]

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
