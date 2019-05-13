package delta.mongo

import java.util.Arrays

import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.{ ClassTag, classTag }

import org.bson.Document

import com.mongodb.MongoWriteException
import com.mongodb.async.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult

import delta.SnapshotStore
import delta.process.Exceptions
import scuff.Codec

class MongoSnapshotStore[K: ClassTag, V](
    snapshotCodec: Codec[V, Document],
    coll: MongoCollection[Document])(
    implicit ec: ExecutionContext)
  extends SnapshotStore[K, V] {

  protected val keyClass = {
    val keyClass = classTag[K].runtimeClass
    (if (keyClass.isPrimitive) {
        import java.lang.reflect.Array
        Array.get(Array.newInstance(keyClass, 1), 0).getClass
      } else keyClass
    ).asInstanceOf[Class[K]]
  }

  protected def _id(k: K) = new Document("_id", k)

  protected def where(key: K, newRev: Int, newTick: Long): Document = {
    val $or = Arrays.asList(
      new Document("revision", new Document("$lt", newRev)),
      new Document()
        .append("revision", newRev)
        .append("tick", new Document("$lte", newTick)))
    _id(key).append("$or", $or)
  }

  def read(key: K): Future[Option[Snapshot]] = {
    withFutureCallback[Document] { callback =>
      coll.find(_id(key)).projection(new Document("_id", 0)).first(callback)
    } map { optDoc =>
      optDoc map { doc =>
        val revision = doc.getInteger("revision")
        val tick = doc.getLong("tick")
        val content = snapshotCodec decode doc.get("data", classOf[Document])
        new Snapshot(content, revision, tick)
      }
    }
  }
  def write(key: K, snapshot: Snapshot): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val upsert = new UpdateOptions().upsert(true)
      val contentDoc = snapshotCodec encode snapshot.content
      val update = new Document("$set", new Document()
        .append("data", contentDoc)
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
            throw Exceptions.writeOlder(key, existing, snapshot)
          case None =>
            sys.error(s"Failed to update snapshot $key, revision ${snapshot.revision}, tick ${snapshot.tick}")
        }
      case _ => Future successful (())
    }
  }

}
