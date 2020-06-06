package delta.mongo

import java.util.Arrays

import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.{ ClassTag, classTag }

import org.bson._

import com.mongodb.MongoWriteException
import com.mongodb.async.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult

import delta.{ Revision, Tick, SnapshotStore }
import delta.process.Exceptions

class MongoSnapshotStore[K: ClassTag, V](
  coll: MongoCollection[BsonDocument])(
  implicit
  snapshotCodec: SnapshotCodec[V],
  ec: ExecutionContext,
  protected val keyConv: K => BsonValue)
extends SnapshotStore[K, V] {

  protected val keyClass = {
    val keyClass = classTag[K].runtimeClass
    (if (keyClass.isPrimitive) {
        import java.lang.reflect.Array
        Array.get(Array.newInstance(keyClass, 1), 0).getClass
      } else keyClass
    ).asInstanceOf[Class[K]]
  }

  protected final def doc(
      name: String, value: BsonValue): BsonDocument =
    new BsonDocument(name, value)
  protected final def doc(
      name1: String, val1: BsonValue,
      name2: String, val2: BsonValue): BsonDocument =
    new BsonDocument(name1, val1).append(name2, val2)
  protected final def doc(
      name1: String, val1: BsonValue,
      name2: String, val2: BsonValue,
      name3: String, val3: BsonValue): BsonDocument =
    new BsonDocument(name1, val1).append(name2, val2).append(name3, val3)

  protected def _id(k: K) = doc("_id", k)

  protected def where(key: K, newRev: Revision, newTick: Tick): BsonDocument = {
    val $or = new BsonArray(
      Arrays.asList(
        doc("revision", doc("$lt", newRev)),
        doc("revision", newRev, "tick", doc("$lte", newTick))))
    _id(key).append("$or", $or)
  }

  def read(key: K): Future[Option[Snapshot]] = {
    withFutureCallback[BsonDocument] { callback =>
      coll.find(_id(key)).projection(doc("_id", 0)).first(callback)
    } map {
      _.map(snapshotCodec.decode)
    }
  }
  def write(key: K, snapshot: Snapshot): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val upsert = new UpdateOptions().upsert(true)
      val update = doc("$set", snapshotCodec encode snapshot)
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
      case _ => Future.unit
    }
  }

}
