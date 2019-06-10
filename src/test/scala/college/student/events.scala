package college.student

import college.CollegeEvent

sealed abstract class StudentEvent extends CollegeEvent

case class StudentRegistered(name: String, email: String) extends StudentEvent
case class StudentChangedName(newName: String) extends StudentEvent
case class StudentEmailAdded(email: String) extends StudentEvent
case class StudentEmailRemoved(email: String) extends StudentEvent
