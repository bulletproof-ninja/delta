package delta.testing.mysql

import scala.util.Random
import scala.concurrent._

import delta.Snapshot
import delta.jdbc._, JdbcStreamProcessStore._
import delta.jdbc.mysql.MySQLSyntax
import delta.testing.TestStreamProcessStore
import delta.process._
import delta.testing._

import scuff.jdbc._

class TestMySQLStreamProcessStore
extends TestStreamProcessStore
with MySqlDataSource {
  implicit object StringColumn extends VarCharColumn(255)

  def readModelName = "readmodel_test"

  import TestStreamProcessStore._


  override def newStore(): StreamProcessStore[Long, String, String] = {
    new JdbcStreamProcessStore[Long, String, String](
      Table(readModelName)
        withPrimaryKey "id"
        withTimestamp UpdateTimestamp("last_updated"))
    with MySQLSyntax {
      def connectionSource = TestMySQLStreamProcessStore.this.connSource
    }
  }.ensureTable()

  implicit val fooCodec = new UpdateCodec[Foo, String] {
    def asUpdate(prevState: Option[Foo], currState: Foo): String = Foo encode currState
    def updateState(state: Option[Foo], update: String): Option[Foo] = Some { Foo decode update }
  }

  private val fooTable = s"foo_${Random.nextInt().toHexString}"

  def fooVersion: Short = 1

  object FooProcessStore {
    val indexColumns =
      Index(Nullable[Foo, String]("foo_text") { case Foo(text: String, _) => text } ) ::
      Index(NotNull("foo_num") { foo: Foo => foo.num } )::
      Nil
    implicit val FooColumn = ColumnType(Foo)
  }
  class FooProcessStore(
    protected val connectionSource: AsyncConnectionSource,
    version: Short)(implicit fooCol: ColumnType[Foo])
  extends JdbcStreamProcessStore[Long, Foo, String](
    Table(fooTable)
      withVersion version withTimestamp UpdateTimestamp
      withPrimaryKey "id",
    FooProcessStore.indexColumns) {

    def queryText(text: String): Future[Map[Long, Snapshot]] = {
      this.readSnapshots("foo_text" -> text)
    }

  }
  override def newFooStore = {
    import FooProcessStore._
    val store = new FooProcessStore(connSource, fooVersion) with MySQLSyntax
    store.ensureTable()
  }

  test("foo") {
    val fooStore = newFooStore
    fooStore.write(567567, Snapshot(Foo("Foo", 9999), 99, 999999L))
    val result = fooStore.queryText("Foo").await
    assert(result.nonEmpty)
    result.values.foreach {
      case Snapshot(foo, _, _) =>
        assert("Foo" === foo.text)
    }
  }

}

class TestJdbcStreamProcessHistory
extends TestMySQLStreamProcessStore
with MySqlDataSource {

  override def newStore(): StreamProcessStore[Long, String, String] = newStore(1)
  private def newStore(version: Int) = {
    new JdbcStreamProcessHistory[Long, String, String](
      connSource, version.toShort, UpdateTimestamp, "id", s"${readModelName}_hist")
    with MySQLSyntax
  }.ensureTable()

  private def history(id: Long, store: StreamProcessStore[Long, String, String], dataPrefix: String): Unit = {
    store.write(id, Snapshot("{}", 0, 3L)).await
    store.refresh(id, 1, 5L).await
    assert(Snapshot("{}", 1, 5L) === store.read(id).await.get)

    store.refresh(id, 2, 8).await
    assert(Snapshot("{}", 2, 8L) === store.read(id).await.get)

    try {
      store.write(id, Snapshot(s"$dataPrefix{}", 1, 5)).await
      fail("Should fail on older tick")
    } catch {
      case _: IllegalStateException => // Expected
    }
    val snap_2_8 = Snapshot(s"$dataPrefix{}", 2, 8)
    store.write(id, snap_2_8).await
    assert(Some(snap_2_8) === store.read(id).await)

    val snap_3_34 = Snapshot(s"""$dataPrefix{"hello":", world!"}""", 3, 34)
    store.write(id, snap_3_34).await
    assert(Some(snap_3_34) === store.read(id).await)
  }

  test("history") {
    val id = util.Random.nextLong()
    val store = newStore(1)
    history(id, store, "")

    val store_v2 = newStore(2)
    history(id, store_v2, "JSON: ")
  }

}
