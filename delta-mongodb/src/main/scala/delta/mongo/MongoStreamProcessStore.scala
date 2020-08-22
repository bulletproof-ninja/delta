package delta.mongo

import java.util.ArrayList
import java.util.Collections.singletonList

import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.concurrent._, duration._
import scala.reflect.{ ClassTag, classTag }

import org.bson._

import com.mongodb.ErrorCategory.DUPLICATE_KEY
import com.mongodb.MongoWriteException
import com.mongodb.async.client.MongoCollection
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.UpdateResult

import delta.process._
import scuff.Codec
import scuff.concurrent._
import com.mongodb.client.model.IndexOptions
import java.{util => ju}
import com.mongodb.client.model.IndexModel

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

  def name = coll.getNamespace.getFullName

  def ensureIndexes(
      ensureIndexes: Boolean = true,
      timeout: FiniteDuration = 10.seconds): this.type = {

    if (ensureIndexes) {
      // Don't use sparse indexes, as they won't work for arrays, leading to collection scan
      val start = System.currentTimeMillis
      withBlockingCallback[String](timeout) { cb =>
        coll.createIndex(doc("tick", -1), MongoStreamProcessStore.indexOptions, cb)
      }
      if (secondaryIndexes.nonEmpty) {
        val tickIdxDur = System.currentTimeMillis - start
        withBlockingCallback[ju.List[String]](timeout - tickIdxDur.millis) { cb =>
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

  private def _id(keys: java.util.List[BsonValue]): BsonDocument = doc("_id", doc("$in", new BsonArray(keys)))

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]] =
    withFutureCallback[ArrayList[BsonDocument]] { callback =>
      val keyList = keys.map(k => k: BsonValue).toSeq.asJava
      val target = new ArrayList[BsonDocument](keyList.size)
      coll.find(_id(keyList)).into(target, callback)
    } map {
      case Some(docs) =>
        docs.asScala.foldLeft(Map.empty[K, Snapshot]) {
          case (map, doc) =>
            val key: K = doc.get("_id")
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
    withFutureCallback[UpdateResult] { callback =>
      val update = doc(
        "$set", snapshotCodec encode newSnapshot)
      coll.updateOne(exactly(key, oldSnapshot.revision, oldSnapshot.tick), update, callback)
    } flatMap {
      case Some(upd) if upd.wasAcknowledged && upd.getModifiedCount == 1 => Future.none
      case Some(upd) if upd.getMatchedCount == 0 =>
        read(key) map {
          case found @ Some(_) => found
          case _ => sys.error(s"Failed to replace old snapshot $key, because it doesn't exist!")
        }
      case Some(hmm) => sys.error(s"Unexepected result: $hmm")
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
      doc("$match", doc("matches.1", doc("$exists", true))) ::
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

  private def queryDocs[V](
      fields: Seq[(String, BsonValue)], projection: BsonDocument = null)(
      getValue: BsonDocument => V)
      : Future[Map[K, V]] = {

    val filter: BsonDocument =
      fields
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
    withFutureCallback[ArrayList[BsonDocument]] { callback =>
      val target = new ArrayList[BsonDocument]
      coll.find(filter).projection(projection).into(target, callback)
    } map {
      case Some(docs) =>
        docs.asScala.foldLeft(Map.empty[K, V]) {
          case (map, doc) =>
            val key: K = doc.get("_id")
            val value = getValue(doc)
            map.updated(key, value)
        }
      case _ => Map.empty
    }

  }

  /**
    * Query for snapshots matching column values.
    * Uses `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @param nameValue The name of column or field and associated value to match
    * @param more Addtional `nameValue`s, applied with `AND` semantics
    * @return `Map` of stream ids and snapshot
    */
  protected def queryForSnapshot(
      nameValue: (String, BsonValue), more: (String, BsonValue)*)
      : Future[Map[K, Snapshot]] =
    queryDocs(nameValue +: more)(snapshotCodec.decode)

  /**
    * Lighter version of `queryForSnapshot` if only reference is needed.
    * Uses `AND` semantics, so multiple column
    * queries should not use mutually exclusive
    * values.
    * @param nameValue The name of column or field and associated value to match
    * @param more Addtional `nameValue`s, applied with `AND` semantics
    * @return `Map` of stream ids and tick (in case of duplicates, for chronology)
    */
  protected def queryForTick(
      nameValue: (String, BsonValue), more: (String, BsonValue)*)
      : Future[Map[K, Tick]] =
    queryDocs(nameValue +: more, doc("tick", 1))(_.getInt64("tick").longValue)

}
