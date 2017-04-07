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
import scuff.Codec

class MongoSnapshotStore[K: ClassTag, V](
  snapshotCodec: Codec[V, Document],
  coll: MongoCollection[Document])(
    implicit ec: ExecutionContext)
    extends SnapshotStore[K, V] {

  private[this] val tickIndexFuture = withFutureCallback[String] { cb =>
    coll.createIndex(new Document("tick", -1), cb)
  }

  def maxTick: Future[Option[Long]] = tickIndexFuture.flatMap { _ =>
    withFutureCallback[Document] { cb =>
      coll.find()
        .sort(new Document("tick", -1))
        .limit(1)
        .projection(new Document("_id", 0).append("tick", 1))
        .first(cb)
    } map { maybeDoc =>
      maybeDoc.map(_.getLong("tick"))
    }
  }

  private[this] val keyClass = classTag[K].runtimeClass.asInstanceOf[Class[K]]

  private def where(k: K, newRevision: Int, newTick: Long): Document = {
    val $or = Array(
      new Document("revision", new Document("$lt", newRevision)),
      new Document("tick", new Document("$lt", newTick)))
    _id(k)
      .append("$or", $or)
  }
  private def where(k: K, newRevision: Int): Document = {
    _id(k)
      .append("revision", new Document("$lte", newRevision))
  }

  private def _id(k: K) = new Document("_id", k)
  private def _id(keys: java.util.List[K]): Document = new Document("_id", new Document("$in", keys))

  def read(key: K): Future[Option[Snapshot[V]]] = {
    withFutureCallback[Document] { callback =>
      coll.find(_id(key)).projection(new Document("_id", 0)).first(callback)
    } map { optDoc =>
      optDoc map { doc =>
        val revision = doc.getInteger("revision")
        val tick = doc.getLong("tick")
        val content = snapshotCodec decode doc.get("snapshot", classOf[Document])
        new Snapshot(content, revision, tick)
      }
    }
  }
  def write(key: K, snapshot: Snapshot[V]): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val upsert = new UpdateOptions().upsert(true)
      val contentDoc = snapshotCodec encode snapshot.content
      val doc = _id(key)
        .append("snapshot", contentDoc)
        .append("revision", snapshot.revision)
        .append("tick", snapshot.tick)
      val update = new Document("$set", doc)
      coll.updateOne(where(key, snapshot.revision), update, upsert, callback)
    } map { _ =>
      Unit
    }
  }
  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot[V]]] = {
    withFutureCallback[ArrayList[Document]] { callback =>
      val keyList = keys.toSeq.asJava
      val target = new ArrayList[Document](keyList.size)
      coll.find(_id(keyList)).into(target, callback)
    } map {
      case Some(docs) =>
        docs.asScala.foldLeft(Map.empty[K, Snapshot[V]]) {
          case (map, doc) =>
            val key: K = doc.get("_id", keyClass)
            val revision = doc.getInteger("revision")
            val tick = doc.getLong("tick")
            val content = snapshotCodec decode doc.get("snapshot", classOf[Document])
            map.updated(key, Snapshot(content, revision, tick))
        }
      case _ => Map.empty
    }
  }
  def writeBatch(snapshots: collection.Map[K, Snapshot[V]]): Future[Unit] = {
    val futures = snapshots.map {
      case (key, snapshot) => write(key, snapshot)
    }
    Future.sequence(futures).map(_ => Unit)
  }
  def refresh(key: K, revision: Int, tick: Long): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val updateOnly = new UpdateOptions().upsert(false)
      val update = new Document("$set",
        new Document("revision", revision)
          .append("tick", tick))
      coll.updateOne(where(key, revision, tick), update, updateOnly, callback)
    } map { _ =>
      Unit
    }
  }
  def refreshBatch(revisions: collection.Map[K, (Int, Long)]): Future[Unit] = {
    val futures = revisions.map {
      case (key, (revision, tick)) => refresh(key, revision, tick)
    }
    Future.sequence(futures).map(_ => Unit)
  }

}
