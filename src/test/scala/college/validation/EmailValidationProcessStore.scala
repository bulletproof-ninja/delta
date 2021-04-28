package college.validation

import scala.collection.compat._

import college.student._

import delta.process._

import scuff.EmailAddress

import scala.concurrent._
import college.validation.EmailValidationProcess.State
import scuff.concurrent.Threads.PiggyBack

trait EmailValidationProcessStore
extends StreamProcessStore[Int, State, Unit]
with SecondaryIndexing
with AggregationSupport
with EmailIndex {

  protected def emailRefName: String
  implicit protected def getEmail: MetaType[EmailAddress]

  private implicit def ec = PiggyBack

  def findDuplicates(): Future[Map[EmailAddress, Map[Student.Id, Tick]]] =
    findDuplicates(emailRefName)
      .map {
        _.view.mapValues {
          _.map {
            case (id, tick) => new Student.Id(id) -> tick
          }
        }.toMap
      }

  protected def toQueryValue(addr: EmailAddress): QueryType
  def lookupAll(email: EmailAddress): Future[Map[Student.Id, Tick]] =
    readTicks(emailRefName -> toQueryValue(email))
      .map {
        _.map {
          case (id, tick) => new Student.Id(id) -> tick
        }
      }

}
