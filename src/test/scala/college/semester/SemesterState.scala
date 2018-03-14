package college.semester

import college.StudentId

case class SemesterState(
  name: String,
  enrolled: Set[StudentId] = Set.empty)
