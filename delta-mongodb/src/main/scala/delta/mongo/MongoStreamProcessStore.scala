package delta.mongo

import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

import org.bson.Document

import com.mongodb.ErrorCategory.DUPLICATE_KEY
import com.mongodb.MongoWriteException
import com.mongodb.async.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult

import delta.Snapshot
import delta.util.StreamProcessStore
import scuff.Codec

class MongoStreamProcessStore[K: ClassTag, V](
    snapshotCodec: Codec[V, Document],
    coll: MongoCollection[Document])(
    implicit ec: ExecutionContext)
  extends MongoSnapshotStore(snapshotCodec, coll)
  with StreamProcessStore[K, V] {

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

  private def _id(keys: java.util.List[K]): Document = new Document("_id", new Document("$in", keys))

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]] = {
    withFutureCallback[ArrayList[Document]] { callback =>
      val keyList = keys.toSeq.asJava
      val target = new ArrayList[Document](keyList.size)
      coll.find(_id(keyList)).into(target, callback)
    } map {
      case Some(docs) =>
        docs.asScala.foldLeft(Map.empty[K, Snapshot]) {
          case (map, doc) =>
            val key = doc.get("_id", keyClass)
            val revision = doc.getInteger("revision")
            val tick = doc.getLong("tick")
            val content = snapshotCodec decode doc.get("data", classOf[Document])
            map.updated(key, Snapshot(content, revision, tick))
        }
      case _ => Map.empty
    }
  }

  def writeBatch(snapshots: collection.Map[K, Snapshot]): Future[Unit] = {
    val futures = snapshots.map {
      case (key, snapshot) => write(key, snapshot)
    }
    Future.sequence(futures).map(_ => Unit)
  }

  private def exactly(key: K, oldRev: Int, oldTick: Long): Document =
    _id(key)
      .append("revision", oldRev)
      .append("tick", oldTick)

  def refresh(key: K, revision: Int, tick: Long): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val updateOnly = new UpdateOptions().upsert(false)
      val update = new Document(
        "$set",
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

  protected def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]] = {
    withFutureCallback[Void] { callback =>
      val insert = _id(key)
        .append("data", snapshotCodec encode snapshot.content)
        .append("revision", snapshot.revision)
        .append("tick", snapshot.tick)
      coll.insertOne(insert, callback)
    } map {
      _ => None
    } recoverWith {
      case we: MongoWriteException if we.getError.getCategory == DUPLICATE_KEY =>
        read(key).map {
          case found @ Some(_) => found
          case _ => sys.error(s"Failed to write $key, but cannot find document")
        }
    }
  }
  protected def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]] = {
    withFutureCallback[UpdateResult] { callback =>
      val update = new Document(
        "$set", new Document()
          .append("data", snapshotCodec encode newSnapshot.content)
          .append("revision", newSnapshot.revision)
          .append("tick", newSnapshot.tick))
      coll.updateOne(exactly(key, oldSnapshot.revision, oldSnapshot.tick), update, callback)
    } flatMap {
      case Some(upd) if upd.wasAcknowledged && upd.getModifiedCount == 1 => Future successful None
      case Some(upd) if upd.getMatchedCount == 0 =>
        read(key) map {
          case found @ Some(_) => found
          case _ => sys.error(s"Failed to replace old snapshot $key, because it doesn't exist!")
        }
      case Some(hmm) => sys.error(s"Unexepected result: $hmm")
    }
  }

}
