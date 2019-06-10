package college

import college.student.Student
import scuff.EmailAddress
import scala.concurrent.Future

trait LookupService {
  def findStudent(email: EmailAddress): Future[Option[Student.Id]]
}
