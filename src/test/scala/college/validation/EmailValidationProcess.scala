package college.validation

import college._
import college.student._

import delta.process.PersistentMonotonicProcessing
import delta.util.json.JSON

import scala.concurrent._, duration._

import java.util.concurrent.ScheduledExecutorService

import EmailValidationProcess._

import delta.validation.EventStoreValidationProcess
import delta.validation.EntityCompensation
import delta.write.Metadata
import delta.process.StreamProcessStore
import delta.process.UpdateHub
import scuff.EmailAddress

class EmailValidationProcess(
  protected val tickWindow: Int,
  protected val processStore: StreamProcessStore[Int, State, Unit],
  protected val replayMissingScheduler: ScheduledExecutorService)(
  studentRepo: StudentRepo,
  emailIndex: EmailIndex,
  msgHub: UpdateHub[Int, Unit])(
  implicit
  protected val adHocContext: ExecutionContext,
  metadata: () => Metadata)
extends PersistentMonotonicProcessing[Int, StudentEvent, State, Unit]
with EventStoreValidationProcess[Int, StudentEvent, State] {

  val compensation = {
    case Student.channel =>
        new EntityCompensation(
          StudentIdCodec,
          studentRepo,
          new UniqueStudentEmailValidation(emailIndex))
  }

  protected def replayPersistenceBatchSize: Int = 100
  protected def reportFailure(th: Throwable): Unit = th.printStackTrace(System.err)
  protected def replayMissingDelay: FiniteDuration = 2.seconds
  protected def postReplayTimeout: FiniteDuration = 10.seconds

  protected def onUpdate(id: Int, update: Update): Unit =
    msgHub.publish(id, update)

  protected def selector(es: EventSource): es.Selector = es.ChannelSelector(Student.channel)

  protected def process(tx: Transaction, currState: Option[State]): Future[State] =
    Projector(tx, currState)

}

object EmailValidationProcess {

  import scuff.json._

  object State {
    def apply(emails: Set[EmailAddress]): State = new Data(emails)
    def apply(json: JSON): State = new Json(json)
  }
  sealed abstract class State {
    def asJson: Json
    def asData: Data
  }
  case class Data(allEmails: Set[EmailAddress] = Set.empty, newEmails: Set[EmailAddress] = Set.empty) extends State {
    def asJson: Json = Json(JsArr(allEmails.toSeq.map(_.toLowerCase).map(JsStr(_)): _*).toJson)
    def asData: Data = this
  }
  case class Json(value: JSON) extends State {
    def asJson: Json = this
    def asData: Data = {
      val emails = JsVal.parse(value).asArr.values.map(_.asStr).map(EmailAddress(_))
      Data(allEmails = emails.toSet)
    }
  }

  object Projector extends delta.Projector[State, StudentEvent] {
    def init(evt: StudentEvent): State = next(new Data, evt)

    def next(state: State, evt: StudentEvent): State = evt match {
      case StudentEmailAdded(newEmailAddr) =>
        val newEmail = EmailAddress(newEmailAddr)
        val data = state.asData
        data.copy(
          allEmails = data.allEmails + newEmail,
          newEmails = data.newEmails + newEmail)
      case StudentEmailRemoved(removeEmailAddr) =>
        val removedEmail = EmailAddress(removeEmailAddr)
        val data = state.asData
        data.copy(
          allEmails = data.allEmails - removedEmail,
          newEmails = data.newEmails - removedEmail)
      case _ => // Ignore all other events
        state
    }

  }
}
