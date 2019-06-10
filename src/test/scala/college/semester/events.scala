package college.semester

import college.CollegeEvent
import college.student.Student

sealed abstract class SemesterEvent extends CollegeEvent

case class ClassCreated(className: String) extends SemesterEvent
case class StudentEnrolled(student: Student.Id) extends SemesterEvent
case class StudentCancelled(studentId: Student.Id) extends SemesterEvent
