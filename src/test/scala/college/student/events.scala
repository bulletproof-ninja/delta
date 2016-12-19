package college.student

import college.CollegeEvent

sealed trait StudentEvent extends CollegeEvent

case class StudentRegistered(name: String) extends StudentEvent
case class StudentChangedName(newName: String) extends StudentEvent
