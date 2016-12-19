package college.semester

import college.StudentId

case class State(
  name: String,
  enrolled: Set[StudentId] = Set.empty)
