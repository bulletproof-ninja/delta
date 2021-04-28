package delta.mongo

import java.util.ArrayList

import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.concurrent._, duration._
import scala.reflect.{ ClassTag, classTag }

import org.bson._

import com.mongodb.Block
import com.mongodb.ErrorCategory.DUPLICATE_KEY
import com.mongodb.MongoWriteException
import com.mongodb.async.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.IndexModel
import com.mongodb.client.result.UpdateResult

import delta.process._

import scuff.Codec
import scuff.Reduction
import scuff.concurrent._

import java.{util => ju}
import scala.util.control.NonFatal

object MongoStreamProcessStore {

  private def indexOptions = new IndexOptions().background(false).sparse(false).unique(false)

  private def toIndexModels(indexFields: Seq[String]): List[IndexModel] =
    indexFields.iterator.map { path =>
      val key = new BsonDocument(path, 1)
      new IndexModel(key, indexOptions)
    }.toList

}

class MongoStreamProcessStore[K: ClassTag, S, U](
  coll: MongoCollection[BsonDocument],
  secondaryIndexes: List[IndexModel])(
  implicit
  keyCodec: Codec[K, BsonValue],
  snapshotCodec: SnapshotCodec[S],
  ec: ExecutionContext,
  protected val updateCodec: UpdateCodec[S, U])
extends MongoSnapshotStore(coll)(classTag[K], snapshotCodec, ec, keyCodec.encode)
with StreamProcessStore[K, S, U] with NonBlockingCASWrites[K, S, U]
with SecondaryIndexing
with AggregationSupport {

  def this(
      coll: MongoCollection[BsonDocument],
      secondaryIndexFields: String*)(
      implicit
      keyCodec: Codec[K, BsonValue],
      snapshotCodec: SnapshotCodec[S],
      ec: ExecutionContext,
      updateCodec: UpdateCodec[S, U]) =
    this(coll, MongoStreamProcessStore.toIndexModels(secondaryIndexFields))

  protected implicit def fromBson(bson: BsonValue): K = keyCodec decode bson
  protected implicit def toBsonString(values: Iterable[String]) = values.map(new BsonString(_))
  protected implicit def toBsonInt32(values: Iterable[Int]) = values.map(new BsonInt32(_))
  protected implicit def toBsonInt64(values: Iterable[Long]) = values.map(new BsonInt64(_))

  protected def $in(matchValues: BsonArray): BsonDocument =
    new BsonDocument("$in", matchValues)
  protected def $in(matchValues: Iterable[BsonValue]): BsonDocument =
    $in(new BsonArray(matchValues.toList.asJava))
  protected def $exists(boolean: Boolean = true): BsonDocument =
    new BsonDocument("$exists", BsonBoolean valueOf boolean)

  def name = coll.getNamespace.getFullName

  def ensureIndexes(
      ensureIndexes: Boolean = true,
      blockingTimeout: FiniteDuration = 120.seconds): this.type = {

    if (ensureIndexes) {
      // Don't use sparse indexes, as they won't work for arrays, leading to collection scan
      val start = System.currentTimeMillis
      withBlockingCallback[String](blockingTimeout) { cb =>
        coll.createIndex(doc("tick", -1), MongoStreamProcessStore.indexOptions, cb)
      }
      if (secondaryIndexes.nonEmpty) {
        val tickIdxDur = System.currentTimeMillis - start
        withBlockingCallback[ju.List[String]](blockingTimeout - tickIdxDur.millis) { cb =>
          coll.createIndexes(secondaryIndexes.asJava, cb)
        }
      }
    }

    this
  }

  def tickWatermark: Option[Tick] = {
    val future =
      withFutureCallback[BsonDocument] { cb =>
        coll.find()
          .sort(doc("tick", -1))
          .limit(1)
          .projection(doc("_id", 0).append("tick", 1))
          .first(cb)
      } map { maybeDoc =>
        maybeDoc.map(_.getInt64("tick").longValue)
      }
    blocking(future await 11.seconds)
  }

  private def _id(keys: java.util.List[BsonValue]): BsonDocument = doc("_id", $in(new BsonArray(keys)))

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]] =
    withFutureCallback[ArrayList[BsonDocument]] { callback =>
      val keyList = keys.map(k => k: BsonValue).toSeq.asJava
      val target = new ArrayList[BsonDocument](keyList.size)
      coll.find(_id(keyList)).into(target, callback)
    } map {
      case Some(docs) =>
        docs.asScala.foldLeft(Map.empty[K, Snapshot]) {
          case (map, doc) =>
            val key: K = doc remove "_id"
            val snapshot = snapshotCodec decode doc
            map.updated(key, snapshot)
        }
      case _ => Map.empty
    }

  def writeBatch(snapshots: collection.Map[K, Snapshot]): Future[Unit] = {
    val futures = snapshots.map {
      case (key, snapshot) => write(key, snapshot)
    }
    Future.sequence(futures).map(_ => ())
  }

  private def bulkWrite(snapshots: collection.Map[K, Snapshot]): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      coll.bulkW
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


  private def exactly(key: K, oldRev: Revision, oldTick: Tick): BsonDocument =
    _id(key)
      .append("revision", oldRev)
      .append("tick", oldTick)

  def refresh(key: K, revision: Revision, tick: Tick): Future[Unit] = {
    withFutureCallback[UpdateResult] { callback =>
      val updateOnly = new UpdateOptions().upsert(false)
      val update = doc(
        "$set",
        doc("revision", revision)
          .append("tick", tick))
      coll.updateOne(where(key, revision, tick), update, updateOnly, callback)
    }.map(_ => ())
  }
  def refreshBatch(revisions: collection.Map[K, (Int, Long)]): Future[Unit] = {
    val futures = revisions.map {
      case (key, (revision, tick)) => refresh(key, revision, tick)
    }
    Future.sequence(futures).map(_ => ())
  }

  protected def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]] = {
    withFutureCallback[Void] { callback =>
      val insert = snapshotCodec.toDoc(snapshot, _id(key))
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
    val filter = exactly(key, oldSnapshot.revision, oldSnapshot.tick)
    val update = doc(
      "$set", snapshotCodec encode newSnapshot)
    withFutureCallback[UpdateResult] { callback =>
      coll.updateOne(filter, update, callback)
    } flatMap {
      case Some(upd) if upd.wasAcknowledged && upd.getModifiedCount == 1 => Future.none
      case Some(upd) if upd.getModifiedCount == 0 =>
        read(key) map {
          case found @ Some(_) => found
          case _ => sys.error(s"Failed to replace old snapshot $key, because it doesn't exist!")
        }
      case Some(unexpected) => sys.error(s"Unexepected result: $unexpected\n  filter:$filter\n  update:$update")
      case None => sys.error("No update result was returned!")
    }
  }

  protected type MetaType[T] = BsonValue => T

  /**
    * Find duplicates in index.
    * @return `Map` of duplicates
    */
  protected def findDuplicates[D](
      refName: String)(
      implicit
      extractor: BsonValue => D): Future[Map[D, Map[K, Tick]]] = {

    val $refName = s"$$$refName"

    val pipeline =
      doc("$project", doc("tick", 1, refName, 1)) ::
      doc("$unwind", $refName) ::
      doc("$group", doc("_id", $refName, "matches", doc("$push", "$$ROOT"))) ::
      doc("$project", doc("matches._id", 1, "matches.tick", 1)) ::
      doc("$match", doc("matches.1", $exists())) ::
      Nil

    val aggregation = coll.aggregate(pipeline.asJava)

    withFutureCallback[BsonArray] { callback =>
      val target = new BsonArray
      aggregation.into(target, callback)
    } map {
      case Some(docs) =>
        docs.asScala.foldLeft(Map.empty[D, Map[K, Tick]]) {
          case (map, doc: BsonDocument) =>
            val dupe: D = doc get "_id"
            val matches = doc.getArray("matches").asScala
              .foldLeft(Map.empty[K, Tick]) {
                case (map, dup: BsonDocument) =>
                  val key: K = dup get "_id"
                  val tick = dup.getInt64("tick").longValue
                  map.updated(key, tick)
                case (_, bson) => sys.error(s"Unexpected BSON type: $bson")
              }
            map.updated(dupe, matches)
          case (_, bson) => sys.error(s"Unexpected BSON type: $bson")
        }
      case _ => Map.empty
    }

  }

  protected type QueryType = BsonValue

  protected def bulkRead[R](
      filter: (String, QueryType)*)(
      consumer: Reduction[(StreamId, Snapshot), R])
      : Future[R] = {

    val bsonFilter: BsonDocument =
      filter
        .groupBy(_._1)
        .view.mapValues(_.map(_._2))
        .foldLeft(new BsonDocument) {
          case (query, (field, values)) =>
            if (values.size == 1) {
              query.append(field, values.head)
            } else {
              query.append(field, doc("$in", new BsonArray(values.asJava)))
            }
        }
    withFutureCallback[Void] { onComplete =>
      val proxy = new Block[BsonDocument] {
        def apply(doc: BsonDocument): Unit = {
          val id: K = doc remove "_id"
          consumer next id -> snapshotCodec.decode(doc)
        }
      }
      coll.find(bsonFilter).forEach(proxy, onComplete)
    } recover {
      case NonFatal(cause) =>
        throw new RuntimeException(s"BSON query failed: ${bsonFilter.toJson}", cause)
    } map { _ =>
      consumer.result()
    }
  }

  /**
    * Lighter version of `queryForSnapshot` if only reference is needed.
    * Uses `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @param nameValue The name of column or field and associated value to match
    * @param more Addtional `nameValue`s, applied with `AND` semantics
    * @return `Map` of stream ids and tick (in case of duplicates, for chronology)
    */
  protected def readTicks(
      nameValue: (String, BsonValue), more: (String, BsonValue)*)
      : Future[Map[K, Tick]] = {
    // queryDocs(nameValue +: more, doc("tick", 1))(_.getInt64("tick").longValue)
    val filter: BsonDocument =
      (nameValue +: more)
        .groupBy(_._1)
        .view.mapValues(_.map(_._2))
        .foldLeft(new BsonDocument) {
          case (query, (field, values)) =>
            if (values.size == 1) {
              query.append(field, values.head)
            } else {
              query.append(field, doc("$in", new BsonArray(values.asJava)))
            }
        }
    val ticks = new LockFreeConcurrentMap[K, Tick]
    withFutureCallback[Void] { onComplete =>
      val buildMap = new Block[BsonDocument] {
        def apply(doc: BsonDocument): Unit = {
          ticks.update(doc.get("_id"), doc.getInt64("tick").longValue)
        }
      }
      coll.find(filter).projection(doc("tick", 1)).forEach(buildMap, onComplete)
    } map { _ =>
      ticks.snapshot[Map[K, Tick]]
    }

  }
}
