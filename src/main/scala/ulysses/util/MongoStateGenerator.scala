//package ulysses.util
//
//import ulysses._
//import com.mongodb._
//import scuff.Mongolia._
//import concurrent.duration._
//import java.util.Date
//import scala.annotation.implicitNotFound
//import scuff.Timestamp
//
///**
// * Generate state and keep track of handled [[ulysses.EventSource#Transaction]]s, so process can resume
// * after shutdown.
// * Uses this collection format:
// * {{{
// * {  _id: <stream>,
// *    _rev: 123, // Marked revision
// *    _time: <clock>, // Marked transaction clock
// * }
// * }}}
// * @param dbColl MongoDB collection. Set whatever WriteConcern is appropriate before passing
// */
//@implicitNotFound("Cannot find implicit Codec for ID <=> BsonValue")
//final class MongoStateGenerator[ID](dbColl: DBCollection)(implicit idCdc: scuff.Codec[ID, BsonValue]) {
//
//  dbColl.createIndex("_time")
//
//  def lastTimestamp: Option[Long] = {
//    dbColl.find(obj("_time":=$exists(true)), obj("_id" := EXCLUDE, "_time" := INCLUDE)).last("_time").map { doc =>
//      doc("_time").as[Long]
//    }
//  }
//
//  private[this] final val RevReadFields = obj("_id" := EXCLUDE, "_rev" := INCLUDE)
//
//  def expectedRevision(streamId: ID): Int = {
//    dbColl.findOne(obj("_id" := streamId), RevReadFields) match {
//      case null => 0
//      case doc => doc.getAs[Int]("_rev") + 1
//    }
//  }
//
//  /**
//   * Update state and mark revision as processed.
//   * @param streamId Transaction stream id
//   * @param revision Transaction stream revision
//   * @param time Transaction clock
//   * @param update Optional update object.
//   * @param listKey Additional key, only to be used for updating a specific list element
//   */
//  def commitRevision(streamId: ID, revision: Int, time: Long, updateQry: DBObject = obj(), listKey: BsonProp = null) =
//    update(List(streamId), Some(revision -> time), updateQry, listKey).head
//
//  private def update(streamIds: List[ID], revTime: Option[(Int, Long)], updateQry: DBObject, listKey: BsonProp): Iterable[DBObject] = {
//    if (streamIds.isEmpty) {
//      Nil
//    } else {
//      val setModifier = updateQry("$set").opt[DBObject]
//      val useModifiers = setModifier.isDefined || updateQry.isEmpty || updateQry.keys.exists(_ startsWith "$")
//      val definitelyUpdate = revTime.map {
//        case (revision, time) =>
//          require(revision >= 0L, s"Revision for ${streamIds.head} cannot be negative: $revision")
//          require(streamIds.tail.isEmpty, "Cannot commit revision $revision for multiple streams: ${streamIds.mkString}")
//          setModifier match {
//            case Some(setModifier) =>
//              setModifier.add("_rev" := revision, "_time" := new Date(time))
//            case None =>
//              if (useModifiers) {
//                updateQry.add($set("_rev" := revision, "_time" := new Date(time)))
//              } else {
//                updateQry.add("_rev" := revision, "_time" := new Date(time))
//              }
//          }
//          revision != 0L
//      }.getOrElse(false)
//
//        def checkUpdate(key: DBObject, res: WriteResult) {
//          if (res.getN != streamIds.size) {
//            throw new IllegalStateException(s"Update $updateQry failed for key $key. Only ${res.getN} docs updated, but ${streamIds.size} expected.")
//          }
//        }
//
//      if (definitelyUpdate) {
//        streamIds match {
//          case streamId :: Nil =>
//            val idProp = "_id" := streamId
//            val key = obj(idProp)
//            if (useModifiers) {
//              dbColl.updateAndReturn(key, updateQry).toList
//            } else {
//              val res = dbColl.safeUpdate(key, updateQry)
//              checkUpdate(key, res)
//              updateQry.add(idProp) :: Nil
//            }
//          case moreThanOne =>
//            val key = obj("_id" := $in(moreThanOne: _*))
//            val res = dbColl.safeUpdateMulti(key, updateQry)
//            checkUpdate(key, res)
//            dbColl.find(key).toSeq
//        }
//      } else { // Unknown if update or insert
//        streamIds.map { streamId =>
//          val idProp = "_id" := streamId
//          val key = obj(idProp)
//          if (useModifiers) {
//            dbColl.upsertAndReturn(key, updateQry)
//          } else {
//            updateQry.add(idProp)
//            val res = dbColl.safeUpsert(key, updateQry)
//            checkUpdate(key, res)
//            val copy = obj()
//            copy.putAll(updateQry)
//            copy
//          }
//        }
//      }
//    }
//  }
//
//  /**
//   * Update state, but do not mark with revision.
//   */
//  def updateState(streamId: ID, updateQry: DBObject, listKey: BsonProp = null): DBObject = update(List(streamId), None, updateQry, listKey).head
//  /**
//   * Update multiple states, but do not mark with revision.
//   */
//  def updateStates(streamIds: Set[ID], updateQry: DBObject, listKey: BsonProp = null): Iterable[DBObject] = update(streamIds.toList, None, updateQry, listKey)
//
//}
