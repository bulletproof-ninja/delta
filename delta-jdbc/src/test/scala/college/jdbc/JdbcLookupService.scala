package college.jdbc

import scala.concurrent.Future

import college.LookupService
import college.student.Student
import scuff.EmailAddress
import scuff.concurrent.Threads

class JdbcLookupService(store: StudentEmailsStore)
  extends LookupService {

  private implicit def ec = Threads.PiggyBack

  def findStudent(email: EmailAddress): Future[Option[Student.Id]] = {
    store.lookup(email.toLowerCase).map(_.map(new Student.Id(_)))
  }
}
