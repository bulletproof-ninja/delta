package college.semester

import college.student.Student

case class SemesterState(
  name: String,
  enrolled: Set[Student.Id] = Set.empty)
