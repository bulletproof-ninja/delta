package delta.hazelcast

import collection.JavaConverters._
import collection.Map

import com.hazelcast.core.MapStore

import java.util.{
  Collection,
  Map => JMap
}

trait ReadModelMapStore[K, D]
    extends MapStore[K, ReadModelState[D, Any]] {

  def delete(k: K) = ()
  def deleteAll(keys: Collection[K]) = ()

  def store(key: K, state: ReadModelState[D, Any]): Unit = {
    if (state.unapplied.isEmpty) {
      if (state.dataUpdated) storeModel(key, state.model)
      else updateRevision(key, state.model.revision, state.model.tick)
    }
  }
  def storeAll(map: JMap[K, ReadModelState[D, Any]]) = {
    val dataUpdated = map.asScala.collect {
      case (key, ReadModelState(model, true, unapplied)) if unapplied.isEmpty => key -> model
    }
    val revUpdated = map.asScala.collect {
      case (key, ReadModelState(model, false, unapplied)) if unapplied.isEmpty => key -> (model.revision, model.tick)
    }
    if (dataUpdated.nonEmpty) storeModels(dataUpdated)
    if (revUpdated.nonEmpty) updateRevisions(revUpdated)
  }

  def load(key: K): ReadModelState[D, Any] = new ReadModelState(loadModel(key))
  def loadAll(keys: Collection[K]) = {
    val models = loadModels(keys.asScala)
    models.mapValues(m => new ReadModelState[D, Any](m)).asJava
  }

  def loadModel(key: K): ReadModel[D]
  def storeModel(key: K, model: ReadModel[D]): Unit
  def updateRevision(key: K, revision: Int, tick: Long)
  def loadModels(keys: Iterable[K]): Map[K, ReadModel[D]]
  def storeModels(model: Map[K, ReadModel[D]]): Unit
  def updateRevisions(revisions: Map[K, (Int, Long)]): Unit

}
