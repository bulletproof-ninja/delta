package delta.mongo

import delta.SnapshotStore
import scala.concurrent.Future
import delta.Snapshot
import com.mongodb.async.client.MongoCollection
import org.bson.Document
import scala.concurrent.ExecutionContext
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult
import collection.JavaConverters._
import scala.reflect.{ ClassTag, classTag }
import java.util.ArrayList
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistries

object MongoSnapshotStore {
  def apply[K: ClassTag, V](
    codec: Codec[Snapshot[V]],
    coll: MongoCollection[Document])(
      implicit ec: ExecutionContext): MongoSnapshotStore[K, V] = {
    new MongoSnapshotStore(withCodec(codec, coll))
  }
  private def withCodec[V](
    codec: Codec[Snapshot[V]],
    coll: MongoCollection[Document]) = {
    val orgReg = coll.getCodecRegistry
    val addReg = CodecRegistries.fromCodecs(codec)
    val bothReg = CodecRegistries.fromRegistries(orgReg, addReg)
    coll.withCodecRegistry(bothReg)
  }
}

class MongoSnapshotStore[K: ClassTag, V](
  coll: MongoCollection[Document])(
    implicit ec: ExecutionContext)
    extends SnapshotStore[K, V] {

  def this(codec: Codec[Snapshot[V]], coll: MongoCollection[Document])(
    implicit ec: ExecutionContext) =
    this(MongoSnapshotStore.withCodec(codec, coll))

  private[this] val keyClass = classTag[K].runtimeClass.asInstanceOf[Class[K]]

  private def _id(k: K): Document = new Document("_id", k)
  private def _id(keys: java.util.List[K]): Document = new Document("_id", new Document("$in", keys))

  def get(key: K): Future[Option[Snapshot[V]]] = {
    withFutureCallback[Document] { callback =>
      coll.find(_id(key)).projection(new Document("_id", 0)).first(callback)
    } map { optDoc =>
      optDoc.map(doc => doc.get("snapshot", classOf[Snapshot[V]]))
    }
  }
  def set(key: K, snapshot: Snapshot[V]): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val upsert = new UpdateOptions().upsert(true)
      val update = new Document("$set", new Document("snapshot", snapshot))
      coll.updateOne(_id(key), update, upsert, callback)
    } map { _ =>
      Unit
    }
  }
  def getAll(keys: Iterable[K]): Future[Map[K, Snapshot[V]]] = {
    withFutureCallback[ArrayList[Document]] { callback =>
      val keyList = keys.toSeq.asJava
      val target = new ArrayList[Document](keyList.size)
      coll.find(_id(keyList)).into(target, callback)
    } map {
      case Some(docs) =>
        docs.asScala.foldLeft(Map.empty[K, Snapshot[V]]) {
          case (map, doc) =>
            val key: K = doc.get("_id", keyClass)
            val snapshot = doc.get("snapshot", classOf[Snapshot[V]])
            map.updated(key, snapshot)
        }
      case _ => Map.empty
    }
  }
  def setAll(snapshots: collection.Map[K, Snapshot[V]]): Future[Unit] = {
    val futures = snapshots.map {
      case (key, snapshot) => set(key, snapshot)
    }
    Future.sequence(futures).map(_ => Unit)
  }
  def update(key: K, revision: Int, tick: Long): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val updateOnly = new UpdateOptions().upsert(false)
      val update = new Document("$set",
        new Document("snapshot.rev", revision)
          .append("snapshot.tick", tick))
      coll.updateOne(_id(key), update, updateOnly, callback)
    } map { _ =>
      Unit
    }
  }
  def updateAll(revisions: collection.Map[K, (Int, Long)]): Future[Unit] = {
    val futures = revisions.map {
      case (key, (revision, tick)) => update(key, revision, tick)
    }
    Future.sequence(futures).map(_ => Unit)
  }

}
