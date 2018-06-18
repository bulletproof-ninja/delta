package delta.hazelcast

import collection.JavaConverters._

import com.hazelcast.core._
import concurrent.duration._

import scuff.concurrent._

import java.util.{ Collection, Map => JMap }

import java.util.Properties
import com.hazelcast.logging.ILogger
import scala.concurrent.Future
import scala.collection.mutable.ArrayBuffer
import delta.util.StreamProcessStore

/**
  * Hazelcast `MapStore` implementation, using
  * a generic [[delta.util.StreamProcessStore]] as
  * back-end store.
  */
class StreamProcessMapStore[K, T](
  processStore: StreamProcessStore[K, T],
  preloadKeys: Iterable[K] = Set.empty[K],
  awaitTimeout: FiniteDuration = 11.seconds)
    extends MapStore[K, EntryState[T, Any]]
    with MapLoaderLifecycleSupport {

  private def logTimedOutFuture(th: Throwable): Unit =
    logger.warning("Timed-out future eventually failed", th)

  @volatile private[this] var _mapName: String = "<uninitialized>"
  protected def mapName = _mapName
  @volatile private[this] var _logger: ILogger = _
  protected def logger = _logger

  def init(hz: HazelcastInstance, props: Properties, mapName: String): Unit = {
    _logger = hz.getLoggingService.getLogger(s"${getClass.getName}: $mapName")
    _mapName = mapName
  }
  def destroy() = ()

  def loadAllKeys = preloadKeys.asJava

  def delete(k: K): Unit =
    logger.warning(s"Tried to delete $k from $mapName, but ignoring. Override `delete` if needed, or use `evict` instead of `remove`/`delete`.")
  def deleteAll(keys: Collection[K]): Unit =
    logger.warning(s"Tried to delete ${keys.size} keys from $mapName, but ignoring. Override `deleteAll` if needed. Or use `evict` instead of `remove`/`delete`.")

  def store(key: K, state: EntryState[T, Any]): Unit = {
    if (state.unapplied.isEmpty) {
      if (state.contentUpdated) processStore.write(key, state.snapshot).await(awaitTimeout, logTimedOutFuture)
      else processStore.refresh(key, state.snapshot.revision, state.snapshot.tick).await(awaitTimeout, logTimedOutFuture)
    }
  }
  def storeAll(map: JMap[K, EntryState[T, Any]]): Unit = {
    val contentUpdated = map.asScala.collect {
      case (key, EntryState(model, true, unapplied)) if unapplied.isEmpty =>
        key -> model
    }
    val revUpdated: Iterable[(K, Int, Long)] =
      map.asScala.collect {
        case (key, EntryState(model, false, unapplied)) if unapplied.isEmpty =>
          (key, model.revision, model.tick)
      }
    val futures = new ArrayBuffer[Future[_]](contentUpdated.size + revUpdated.size)
    if (contentUpdated.nonEmpty) futures += processStore.writeBatch(contentUpdated)
    if (revUpdated.nonEmpty) futures ++= revUpdated.map{ case (key, rev, tick) => processStore.refresh(key, rev, tick) }
    futures.foreach(_.await(awaitTimeout, logTimedOutFuture))
  }

  def load(key: K): EntryState[T, Any] =
    processStore.read(key).await(awaitTimeout, logTimedOutFuture) match {
      case Some(model) => new EntryState(model)
      case _ => null
    }
  def loadAll(keys: Collection[K]) = {
    val models = processStore.readBatch(keys.asScala).await(awaitTimeout, logTimedOutFuture)
    models.mapValues(m => new EntryState[T, Any](m)).asJava
  }

}
