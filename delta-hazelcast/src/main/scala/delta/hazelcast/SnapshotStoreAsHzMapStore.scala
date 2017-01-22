package delta.hazelcast

import collection.JavaConverters._

import com.hazelcast.core.MapStore
import delta.cqrs.ReadModelStore

import concurrent.duration._

import scuff.concurrent._

import java.util.{ Collection, Map => JMap }
import com.hazelcast.logging.ILogger

class ReadModelMapStore[K, D](
  store: ReadModelStore[K, D])(
    implicit logger: ILogger)
    extends MapStore[K, ReadModelState[D, Any]] {

  protected def DefaultTimeout = 33.seconds

  def loadAllKeys = null

  def delete(k: K) = logger.warning(s"Tried to delete $k, but ignoring. Override method if needed.")
  def deleteAll(keys: Collection[K]) = logger.warning(s"Tried to delete ${keys.size} keys, but ignoring. Override method if needed.")

  def store(key: K, state: ReadModelState[D, Any]): Unit = {
    if (state.unapplied.isEmpty) {
      if (state.dataUpdated) store.set(key, state.model).await(DefaultTimeout)
      else store.update(key, state.model.revision, state.model.tick).await(DefaultTimeout)
    }
  }
  def storeAll(map: JMap[K, ReadModelState[D, Any]]) = {
    val dataUpdated = map.asScala.collect {
      case (key, ReadModelState(model, true, unapplied)) if unapplied.isEmpty => key -> model
    }
    val revUpdated = map.asScala.collect {
      case (key, ReadModelState(model, false, unapplied)) if unapplied.isEmpty => key -> (model.revision, model.tick)
    }
    if (dataUpdated.nonEmpty) store.setAll(dataUpdated).await(DefaultTimeout)
    if (revUpdated.nonEmpty) store.updateAll(revUpdated).await(DefaultTimeout)
  }

  def load(key: K): ReadModelState[D, Any] = store.get(key).await(DefaultTimeout) match {
    case Some(model) => new ReadModelState(model)
    case _ => null
  }
  def loadAll(keys: Collection[K]) = {
    val models = store.getAll(keys.asScala).await(DefaultTimeout)
    models.mapValues(m => new ReadModelState[D, Any](m)).asJava
  }

}
