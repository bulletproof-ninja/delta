package delta.hazelcast

import collection.JavaConverters._

import com.hazelcast.core._
import concurrent.duration._

import scuff.concurrent._

import java.util.{ Collection, Map => JMap }

import delta.SnapshotStore
import java.util.Properties
import com.hazelcast.logging.ILogger
import scala.concurrent.Future

/**
  * Hazelcast `MapStore` implementation, using
  * a `SnapshotStore` as back-end store.
  * NOTE: The `ImapAsSnapshotStore` is not a suitable implementation,
  * and is not allowed.
  */
class SnapshotStoreAsHzMapStore[K, T](
  store: SnapshotStore[K, T],
  preloadKeys: Iterable[K] = Set.empty[K],
  awaitTimeout: FiniteDuration = 11.seconds)
    extends MapStore[K, EntryState[T, Any]]
    with MapLoaderLifecycleSupport {

  store match {
    case _: IMapAsSnapshotStore[_, _] => throw new IllegalArgumentException(
      s"Trying to use ${store.getClass.getSimpleName} as SnapshotStore implementation. Think about it. It makes no sense.")
    case _ => // Ok, good.
  }

  @volatile private[this] var _mapName: String = "<unknown>"
  protected def mapName = _mapName
  @volatile private[this] var _logger: ILogger = _
  protected def logger = _logger

  def init(hz: HazelcastInstance, props: Properties, mapName: String) {
    _logger = hz.getLoggingService.getLogger(s"${getClass.getName}: $mapName")
    _mapName = mapName
  }
  def destroy() = ()

  def loadAllKeys = preloadKeys.asJava

  def delete(k: K) =
    logger.warning(s"Tried to delete $k from $mapName, but ignoring. Override method if needed. Or use 'evict' instead of 'remove'.")
  def deleteAll(keys: Collection[K]) =
    logger.warning(s"Tried to delete ${keys.size} keys from $mapName, but ignoring. Override method if needed. Or use 'evict' instead of 'remove'.")

  def store(key: K, state: EntryState[T, Any]): Unit = {
    if (state.unapplied.isEmpty) {
      if (state.contentUpdated) store.set(key, state.snapshot).await(awaitTimeout)
      else store.update(key, state.snapshot.revision, state.snapshot.tick).await(awaitTimeout)
    }
  }
  def storeAll(map: JMap[K, EntryState[T, Any]]) = {
    val contentUpdated = map.asScala.collect {
      case (key, EntryState(model, true, unapplied)) if unapplied.isEmpty => key -> model
    }
    val revUpdated = map.asScala.collect {
      case (key, EntryState(model, false, unapplied)) if unapplied.isEmpty => key -> (model.revision -> model.tick)
    }
    var futures: List[Future[_]] = Nil
    if (contentUpdated.nonEmpty) futures ::= store.setAll(contentUpdated)
    if (revUpdated.nonEmpty) futures ::= store.updateAll(revUpdated)
    futures.foreach(_.await(awaitTimeout))
  }

  def load(key: K): EntryState[T, Any] =
    store.get(key).await(awaitTimeout) match {
      case Some(model) => new EntryState(model)
      case _ => null
    }
  def loadAll(keys: Collection[K]) = {
    val models = store.getAll(keys.asScala).await(awaitTimeout)
    models.mapValues(m => new EntryState[T, Any](m)).asJava
  }

}
