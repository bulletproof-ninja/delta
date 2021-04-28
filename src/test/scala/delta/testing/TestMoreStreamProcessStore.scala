package delta.testing

import scala.collection.compat._

import scala.concurrent.Future
import delta.process._

import scuff._
import scala.util.Random

object TestMoreStreamProcessStore {

  type Snapshot = delta.Snapshot[Contact]

  trait DupeFinder {
      store: StreamProcessStore[Long, _, _] with AggregationSupport =>

    protected def emailRef: String
    implicit protected def getEmail: MetaType[EmailAddress]

    def findDuplicateEmails()
        : Future[Map[EmailAddress, Map[Long, Tick]]] =
      store.findDuplicates[EmailAddress](emailRef)

  }

  trait Lookup {
      store: StreamProcessStore[Long, _, _] with SecondaryIndexing =>

    protected def emailRef: String
    protected def numRef: String
    protected def num2qry(num: Int): QueryType
    protected def email2qry(email: EmailAddress): QueryType

    def findByNumber(num: Int): Future[Map[Long, Snapshot]] =
      store.readSnapshots(numRef -> num2qry(num))
    def findTickByNumber(num: Int): Future[Map[Long, Tick]] =
      store.readTicks(numRef -> num2qry(num))
    def findByEmail(email: EmailAddress): Future[Map[Long, Snapshot]] =
      store.readSnapshots(emailRef -> email2qry(email))
    def findTickByEmail(email: EmailAddress): Future[Map[Long, Tick]] =
      store.readTicks(emailRef -> email2qry(email))
  }

  object ContactCodec extends Codec[Contact, String] {
    def encode(contact: Contact) = s"${contact.email}:${contact.num}"
    def decode(str: String): Contact = {
      val Array(email, num) = str.split(":")
      Contact(EmailAddress(email), num.toInt)
    }
  }
}

class TestMoreStreamProcessStore
extends BaseTest {

  def numContacts = 10000L

  import TestMoreStreamProcessStore._

  type ReplayState = delta.process.ReplayState[String]

  def newDupeStore()
      : StreamProcessStore[Long, Contact, Contact] with DupeFinder =
    new InMemoryProcStore[Long, Contact, Contact]("test-store")
    with DupeFinder {
      protected final val emailRef: String = "The e-mail"
      protected val getEmail = (ref: String, contact: Contact) => {
        assert(ref == emailRef)
        Set(contact.email)
      }
    }

  def newLookupStore()
      : StreamProcessStore[Long, Contact, Contact] with Lookup =
    new InMemoryProcStore[Long, Contact, Contact]("test-store")
    with Lookup {
      override protected def isQueryMatch(name: String, value: Any, contact: Contact): Boolean = {
        name match {
          case `emailRef` => contact.email == value
          case `numRef` => contact.num == value
          case _ => super.isQueryMatch(name, value, contact)
        }
      }
      protected final val emailRef: String = "The e-mail"
      protected val emailTypeRef = (ref: String, contact: Contact) => {
        assert(ref == emailRef)
        Set(contact.email)
      }
      protected final val numRef: String = "The number"
      protected def num2qry(num: Int): QueryType = num
      protected def email2qry(email: EmailAddress): QueryType = email

    }

  test("findNumber") {
    val store = newLookupStore()
    val foos = (1L to numContacts).map { tick =>
      new Snapshot(Contact(num = Random.nextBetween(0 -> 100)), 0, tick)
    }
    val batchWrite = store writeBatch foos.foldLeft(Map.empty[Long, Snapshot]) {
      case (map, snapshot) => map.updated(Random.nextLong(), snapshot)
    }
    batchWrite.await

    val matches42 = store.findByNumber(42).await
    assert(matches42.nonEmpty, "The num 42 was not found at all")
    assert(matches42.forall(_._2.state.num == 42))
    val tickMatches42 = store.findTickByNumber(42).await
    assert(matches42.size === tickMatches42.size)
    matches42.foreach {
      case (id, delta.Snapshot(Contact(_, num), _, tick)) =>
        assert(tick === tickMatches42(id))
        assert(42 === num)
    }
  }

  test("findEmail") {
    val store = newLookupStore()
    val foos = (1L to numContacts).map { tick =>
      new Snapshot(Contact(num = Random.nextBetween(0 -> 100)), 0, tick)
    }
    val batchWrite = store writeBatch foos.foldLeft(Map.empty[Long, Snapshot]) {
      case (map, snapshot) => map.updated(Random.nextLong(), snapshot)
    }
    val dupeEmail = foos.last.state.email
    store.write(Random.nextLong(), new Snapshot(Contact(dupeEmail, 111), 0, 999)).await
    batchWrite.await

    val noMatch = store.findByEmail(EmailAddress("hello@world.com")).await
    assert(noMatch.isEmpty)

    val oneMatch = store.findByEmail(foos.head.state.email).await
    assert(1 === oneMatch.size)
    assert(foos.head === oneMatch.head._2)

    val twoMatches = store.findByEmail(dupeEmail).await
    assert(2 === twoMatches.size)
    twoMatches.values.foreach {
      case delta.Snapshot(Contact(email, _), _, _) =>
        assert(dupeEmail === email)
    }

    val tickTwoMatches = store.findTickByEmail(dupeEmail).await
    assert(2 === tickTwoMatches.size)
    twoMatches.foreach {
      case (id, delta.Snapshot(_, _, tick)) =>
        assert(tick === tickTwoMatches(id))
    }

  }

  test("findDupes") {
    val store = newDupeStore()

    val dupeEmails = store.findDuplicateEmails().await
    assert(dupeEmails.isEmpty)

    val foos = (1L to numContacts).map { tick =>
      new Snapshot(Contact(num = Random.nextBetween(0 -> 100)), 0, tick)
    }
    val batchWrite = store writeBatch foos.foldLeft(Map.empty[Long, Snapshot]) {
      case (map, snapshot) => map.updated(Random.nextLong(), snapshot)
    }
    val dupeEmail = EmailAddress("hello@world.com")
    store.write(Random.nextLong(), new Snapshot(Contact(dupeEmail, 111), 0, 888)).await
    store.write(Random.nextLong(), new Snapshot(Contact(dupeEmail, 222), 0, 999)).await
    batchWrite.await

    val allDupes = store.findDuplicateEmails().await
    assert(allDupes.size >= 1)
    allDupes.values.foreach {
      case dupes => assert(dupes.size > 1)
    }
    val theDupe = allDupes(dupeEmail)
    assert(2 === theDupe.size)
    val ticks = theDupe.values.toList.sorted
    assert(888L :: 999L :: Nil === ticks)
  }

}
