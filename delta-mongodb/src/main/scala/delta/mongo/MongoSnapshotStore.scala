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
import java.util.Arrays
import com.mongodb.MongoWriteException

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
      val update = new Document("$set", new Document()
        .append("snapshot", contentDoc)
        .append("revision", snapshot.revision)
        .append("tick", snapshot.tick))
      coll.updateOne(where(key, snapshot.revision, snapshot.tick), update, upsert, callback)
    } recover {
      case we: MongoWriteException if we.getError.getCode == 11000 => Some {
        UpdateResult.acknowledged(0, 0, null)
      }
    } flatMap {
      case Some(res: UpdateResult) if res.wasAcknowledged && res.getMatchedCount == 0 && res.getUpsertedId == null =>
        read(key) map {
          case Some(existing) =>
            throw SnapshotStore.Exceptions.writeOlderRevision(key, existing, snapshot)
          case None =>
            sys.error(s"Failed to update snapshot $key, revision ${snapshot.revision}, tick ${snapshot.tick}")
        }
      case _ => Future successful (())
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

  private def where(key: K, newRev: Int, newTick: Long): Document = {
    val $or = Arrays.asList(
      new Document("revision", new Document("$lt", newRev)),
      new Document()
        .append("revision", newRev)
        .append("tick", new Document("$lte", newTick)))
    _id(key).append("$or", $or)
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
