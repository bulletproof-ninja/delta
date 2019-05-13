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

  def readMinTick(id: ID, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = readMinTick(id, minTick, defaultReadTimeout)

  def readMinRevision(id: ID, minRevision: Int)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = readMinRevision(id, minRevision, defaultReadTimeout)

}
