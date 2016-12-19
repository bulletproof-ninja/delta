package college.semester

import college.CollegeEvent
import college.StudentId

sealed trait SemesterEvent extends CollegeEvent

case class ClassCreated(className: String) extends SemesterEvent
case class StudentEnrolled(student: StudentId) extends SemesterEvent
case class StudentCancelled(studentId: StudentId) extends SemesterEvent
