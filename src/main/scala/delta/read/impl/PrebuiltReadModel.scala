package delta.read.impl

import scala.concurrent._, duration._
import delta.read._
import java.util.concurrent.ScheduledExecutorService
import delta.SnapshotStore
import scala.reflect.{ ClassTag, classTag }

/**
 * Read model that relies on `snapshotStore` being pre-built
 * and continuously updated, by another thread or process.
 * The subscription implementation is left out, but can be
 * easily augmented by adding [[delta.read.MessageHubSupport]]
 * trait to an instance of this class.
 * @tparam ID The specific identifier type
 * @tparam ESID The more general EventSource identifier type
 * @tparam SS The snapshot store state type
 * @tparam S The read model type, often the same as `SS`
 */
abstract class PrebuiltReadModel[ID, ESID, SS, S >: SS: ClassTag](
    protected val snapshotStore: SnapshotStore[ESID, SS],
    protected val scheduler: ScheduledExecutorService,
    defaultReadTimeout: FiniteDuration = DefaultReadTimeout)(
    implicit
    convId: ID => ESID)
  extends BasicReadModel[ID, S]
  with SubscriptionSupport[ID, S]
  with SnapshotStoreSupport[ID, ESID, SS, S] {

  protected val stateClass = classTag[S].runtimeClass.asInstanceOf[Class[S]]
  protected def idConv(id: ID): ESID = convId(id)

  protected def readStrict(id: ID, expected: Either[Long, Int])(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    expected match {
      case Right(minRev) => readLatest(id).flatMap(verifyRevision(id, _, minRev))
      case Left(minTick) => readLatest(id).flatMap(verifyTick(id, _, minTick))
    }

  def readMinTick(id: ID, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = readMinTick(id, minTick, defaultReadTimeout)

  def readMinRevision(id: ID, minRevision: Int)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = readMinRevision(id, minRevision, defaultReadTimeout)

}
