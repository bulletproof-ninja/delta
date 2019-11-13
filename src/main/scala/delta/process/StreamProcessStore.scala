package delta.process

import collection.Map
import delta.SnapshotStore
import scala.concurrent._
import scala.concurrent.duration._
import scuff.concurrent._
import scala.Right
import scuff.Codec
import scala.reflect.{ classTag, ClassTag }

trait StreamProcessStore[K, S] extends SnapshotStore[K, S] {

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]]
  def writeBatch(batch: Map[K, Snapshot]): Future[Unit]
  def refresh(key: K, revision: Int, tick: Long): Future[Unit]
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit]

  /**
   *  Tick watermark. This is a best-effort
   *  watermark, usually either the highest
   *  or last processed. Combined with a
   *  potential tick skew, this is used to
   *  determine where to resume any interrupted
   *  stream processing.
   */
  def tickWatermark: Option[Long]

  type SnapshotUpdate = delta.process.SnapshotUpdate[S]

  /**
   * Update/Insert.
   * @param key The key to update
   * @param updateThunk The update function. Return `None` if no insert/update is desired
   * @param updateContext Execution context for running the update thunk
   * @return The result of the `updateThunk` function
   */
  def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[SnapshotUpdate], R)]

}

/**
 * Trait supporting side-effecting stream
 * processing.
 * NOTE: Side-effects are always at-least-once semantics.
 */
trait SideEffects[K, S]
  extends StreamProcessStore[K, S] {

  /** Perform side-effect, if necessary, and return final state. */
  protected def doSideEffect(state: S): Future[S]

  @inline
  private def replaceContent(snapshot: Snapshot, newContent: S): Snapshot =
    if (snapshot contentEquals newContent) snapshot
    else new Snapshot(newContent, snapshot.revision, snapshot.tick)

  final abstract override def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[SnapshotUpdate], R)] = {
    super.upsert(key) { optSnapshot =>
      updateThunk(optSnapshot).flatMap {
        case (Some(snapshot), r) =>
          doSideEffect(snapshot.content)
            .map(state => Some(replaceContent(snapshot, state)) -> r)(Threads.PiggyBack)
        case t =>
          Future successful t
      }
    }
  }

  final abstract override def write(key: K, snapshot: Snapshot): Future[Unit] = {
    doSideEffect(snapshot.content).flatMap { state =>
      super.write(key, replaceContent(snapshot, state))
    }(Threads.PiggyBack)
  }

  final abstract override def writeBatch(batch: Map[K, Snapshot]): Future[Unit] = {
    val futureWrites = batch.map {
      case (key, snapshot) => write(key, snapshot)
    }
      implicit def ec = Threads.PiggyBack
    Future.sequence(futureWrites).map(_ => ())
  }


}

private[delta] object StreamProcessStore {
  val UnitFuture = Future.successful(())
  val NoneFuture = Future successful None

  def adaptKeys[K1: ClassTag, K2: ClassTag, V](store: StreamProcessStore[K1, V], codec: Codec[K1, K2]): StreamProcessStore[K2, V] =
    adaptKeys(store)(implicitly, implicitly, codec.encode, codec.decode)
  def adaptKeys[K1: ClassTag, K2: ClassTag, V](store: StreamProcessStore[K1, V])(
      implicit
      c1: K1 => K2, c2: K2 => K1): StreamProcessStore[K2, V] = {
    if (classTag[K1] == classTag[K2]) store.asInstanceOf[StreamProcessStore[K2, V]]
    else new StreamProcessStore[K2, V] {
      def read(key: K2): Future[Option[Snapshot]] = store read key
      def write(key: K2, snapshot: Snapshot): Future[Unit] = store.write(key, snapshot)
      def readBatch(keys: Iterable[K2]): Future[scala.collection.Map[K2, Snapshot]] = {
        val result = store readBatch keys.view.map(key => key: K1)
        result.map {
          _.map {
            case (key, value) => (key: K2) -> value
          }
        }(Threads.PiggyBack)
      }
      def refresh(key: K2, revision: Int, tick: Long): Future[Unit] = store.refresh(key, revision, tick)
      def refreshBatch(revisions: Map[K2, (Int, Long)]): Future[Unit] =
        store refreshBatch revisions.map {
          case (key, value) => (key: K1) -> value
        }
      def tickWatermark: Option[Long] = store.tickWatermark
      def upsert[R](key: K2)(updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
          implicit
          updateContext: ExecutionContext): Future[(Option[SnapshotUpdate], R)] = store.upsert(key)(updateThunk)
      def writeBatch(batch: Map[K2, Snapshot]): Future[Unit] =
        store writeBatch batch.map {
          case (key, value) => (key: K1) -> value
        }

    }
  }

}

trait NonBlockingRecursiveUpsert[K, V] {
  store: StreamProcessStore[K, V] =>

  protected type ConflictingSnapshot = Snapshot
  protected type ContentUpdated = Boolean

  /** Update and return updated snapshot, if any. */
  protected def upsertRecursive[R](
      key: K, existing: Option[Snapshot],
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)],
      writeIfExpected: (K, Option[Snapshot], Snapshot) => Future[Either[ConflictingSnapshot, ContentUpdated]])(
      implicit
      updateContext: ExecutionContext): Future[(Option[SnapshotUpdate], R)] = {
    updateThunk(existing).flatMap {
      case (result, payload) =>
        // If no result OR exact same content, revision, and tick
        if (result.isEmpty || result == existing) Future successful None -> payload
        else {
          val Some(newSnapshot) = result // ! isEmpty
          writeIfExpected(key, existing, newSnapshot) flatMap {
            case Right(contentUpdated) => Future successful Some(SnapshotUpdate( /*key,*/ newSnapshot, contentUpdated)) -> payload
            case Left(conflict) => upsertRecursive(key, Some(conflict), updateThunk, writeIfExpected)
          }
        }
    }
  }

}

trait BlockingRecursiveUpsert[K, V] {
  store: StreamProcessStore[K, V] =>

  protected type ConflictingSnapshot = Snapshot
  protected type ContentUpdated = Boolean
  protected def blockingCtx: ExecutionContext

  /**
   * Update and return updated snapshot, if any.
   * NOTE: The `upsertThunk` will be run on the
   * provided execution context, but all other
   * code will stay on the thread used to call
   * this method, by blocking and awaiting
   * `updateThunk`.
   */
  protected def upsertRecursive[R](
      key: K, existing: Option[Snapshot],
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)],
      updateThunkTimeout: FiniteDuration,
      writeIfExpected: (K, Option[Snapshot], Snapshot) => Either[ConflictingSnapshot, ContentUpdated])(
      implicit
      updateContext: ExecutionContext): (Option[SnapshotUpdate], R) = {

    val updated = Future(updateThunk(existing)).flatMap(identity)
    blocking(updated.await(updateThunkTimeout)) match {
      case (result, payload) =>
        if (result.isEmpty || result == existing) None -> payload
        else {
          val Some(newSnapshot) = result // ! isEmpty
          writeIfExpected(key, existing, newSnapshot) match {
            case Right(contentUpdated) => Some(SnapshotUpdate( /*key,*/ newSnapshot, contentUpdated)) -> payload
            case Left(conflict) => upsertRecursive(key, Some(conflict), updateThunk, updateThunkTimeout, writeIfExpected)
          }
        }
    }

  }

}

/**
 * Partial implementation of upsert method,
 * relying on non-blocking atomic `writeIfAbsent` and
 * `writeReplacement` behavior.
 */
trait NonBlockingCASWrites[K, V] extends NonBlockingRecursiveUpsert[K, V] {
  store: StreamProcessStore[K, V] =>

  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]]
  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]]

  def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[SnapshotUpdate], R)] = {

      // Write snapshot, if current expectation holds.
      def writeIfExpected(key: K, expected: Option[Snapshot], snapshot: Snapshot): Future[Either[ConflictingSnapshot, ContentUpdated]] = {
        val contentUpdated = !expected.exists(_.contentEquals(snapshot))
        if (contentUpdated) {
          val potentialWriteConflict = expected match {
            case Some(expected) => writeReplacement(key, expected, snapshot)
            case _ => writeIfAbsent(key, snapshot)
          }
          potentialWriteConflict.map(_.toLeft(right = true))
        } else {
          refresh(key, snapshot.revision, snapshot.tick).map(_ => Right(false))
        }
      }

    read(key).flatMap(upsertRecursive(key, _, updateThunk, writeIfExpected))
  }

}

/**
 * Partial implementation of upsert method,
 * expecting non-thread-safe blocking connection.
 */
trait BlockingCASWrites[K, V, Conn] extends BlockingRecursiveUpsert[K, V] {
  store: StreamProcessStore[K, V] =>

  /** Read existing snapshot for updating. */
  protected def readForUpdate[R](key: K)(thunk: (Conn, Option[Snapshot]) => R): R
  /** Refresh snapshot. */
  protected def refreshKey(conn: Conn)(key: K, revision: Int, tick: Long): Unit
  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(conn: Conn)(key: K, snapshot: Snapshot): Option[Snapshot]
  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(conn: Conn)(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot]

  protected val updateThunkTimeout: FiniteDuration = 11.seconds

  def upsert[R](key: K)(
      updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[SnapshotUpdate], R)] = {

      // Write snapshot, if current expectation holds.
      def writeIfExpected(conn: Conn)(
          key: K, expected: Option[Snapshot], snapshot: Snapshot)
          : Either[ConflictingSnapshot, ContentUpdated] = {
        val contentUpdated = !expected.exists(_ contentEquals snapshot)
        if (contentUpdated) {
          val writeConflict: Option[Snapshot] = expected match {
            case Some(expected) => writeReplacement(conn)(key, expected, snapshot)
            case _ => writeIfAbsent(conn)(key, snapshot)
          }
          writeConflict.toLeft(right = contentUpdated)
        } else {
          refreshKey(conn)(key, snapshot.revision, snapshot.tick)
          Right(false)
        }
      }

    Future {
      readForUpdate(key) {
        case (conn, existing) =>
          upsertRecursive(key, existing, updateThunk, updateThunkTimeout, writeIfExpected(conn))
      }
    }(blockingCtx) // <- MUST run on the blocking context, otherwise will dead-lock
  }

}
class SnapshotStoreAdapter[Ka, Va, Kb, Vb](
    underlying: SnapshotStore[Kb, Vb],
    keyAdapter: scuff.Codec[Ka, Kb],
    valueAdapter: scuff.Codec[Va, Vb])
  extends SnapshotStore[Ka, Va] {

  import scuff.concurrent.Threads.PiggyBack

  @inline
  protected final def ~>(k: Ka): Kb = keyAdapter encode k
  @inline
  protected final def ~>(s: Snapshot): delta.Snapshot[Vb] = s.map(valueAdapter.encode)
  @inline
  protected final def <~(s: delta.Snapshot[Vb]): Snapshot = s.map(valueAdapter.decode)

  def read(key: Ka): Future[Option[Snapshot]] =
    underlying.read(this ~> key).map(_.map(this.<~))(PiggyBack)
  def write(key: Ka, snapshot: Snapshot): Future[Unit] =
    underlying.write(this ~> key, this ~> snapshot)
}

class StreamProcessStoreAdapter[Ka, Va, Kb, Vb](
    underlying: StreamProcessStore[Kb, Vb],
    keyAdapter: scuff.Codec[Ka, Kb],
    valueAdapter: scuff.Codec[Va, Vb])
  extends SnapshotStoreAdapter[Ka, Va, Kb, Vb](underlying, keyAdapter, valueAdapter)
  with StreamProcessStore[Ka, Va] {

  import scuff.concurrent.Threads.PiggyBack

  @inline
  private def <~(k: Kb): Ka = keyAdapter decode k

  def readBatch(keys: Iterable[Ka]): Future[Map[Ka, Snapshot]] =
    underlying.readBatch(keys.map(this.~>)).map { map =>
      map.map {
        case (key, snapshot) =>
          (this <~ key) -> (this <~ snapshot)
      }
    }(PiggyBack)

  def refresh(key: Ka, revision: Int, tick: Long): Future[Unit] =
    underlying.refresh(this ~> key, revision, tick)

  def refreshBatch(revisions: Map[Ka, (Int, Long)]): Future[Unit] =
    underlying refreshBatch revisions.map {
      case (key, revTick) => this ~> key -> revTick
    }

  def writeBatch(batch: Map[Ka, Snapshot]): Future[Unit] =
    underlying writeBatch batch.map {
      case (key, snapshot) =>
        this ~> key -> this ~> snapshot
    }

  def tickWatermark: Option[Long] = underlying.tickWatermark

  def upsert[R](key: Ka)(updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit
      updateContext: ExecutionContext): Future[(Option[SnapshotUpdate], R)] = {
    val result = underlying.upsert[R](this ~> key) { snapshotB =>
      val snapshotA = snapshotB.map(this.<~)
      updateThunk(snapshotA).map(t => t._1.map(this.~>) -> t._2)
    }
    result.map {
      case (optUpd, r) =>
        optUpd.map(upd => SnapshotUpdate( /*key,*/ this <~ upd.snapshot, upd.contentUpdated)) -> r
    }
  }
}
