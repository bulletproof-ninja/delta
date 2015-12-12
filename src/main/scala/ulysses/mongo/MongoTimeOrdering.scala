package ulysses.mongo

import ulysses.TimeOrdering
import com.mongodb.async.client.MongoCollection
import org.bson.Document
import scala.concurrent.Future
import scuff.concurrent.StreamCallback
import org.bson.BsonArray

trait MongoTimeOrdering[ID, EVT, CAT] extends TimeOrdering[ID, EVT, CAT] {
  es: MongoEventStore[ID, EVT, CAT] =>

  withBlockingCallback[String]()(store.createIndex(new Document("time", 1), _))
  withBlockingCallback[String]()(store.createIndex(new Document("category", 1), _))

  private[this] val OrderByTime = new Document("time", 1)

  def latestTimestamp(): Future[Option[Long]] = {
    withFutureCallback[Document] { callback =>
      store.find(new Document, classOf[Document])
        .projection(new Document("time", true).append("_id", false))
        .sort(new Document("time", -1))
        .limit(1)
        .first(callback)
    }.map { optDoc =>
      optDoc.map(_.getLong("time").longValue)
    }
  }

  private def toJList(categories: Seq[CAT]): java.util.List[CAT] = {
    categories.foldLeft(new java.util.ArrayList[CAT](categories.size)) {
      case (list, category) =>
        list add category
        list
    }
  }

  def replay(categories: CAT*)(callback: StreamCallback[Transaction]): Unit = {
    val filter = new Document
    categories.size match {
      case 0 => // Ignore
      case 1 => filter.append("category", categories.head)
      case _ => filter.append("category", new Document("$in", toJList(categories)))
    }
    query(filter, OrderByTime, callback)
  }

  def replayFrom(fromTimestamp: Long, categories: CAT*)(callback: StreamCallback[Transaction]): Unit = {
    val filter = new Document("time", new Document("$gte", fromTimestamp))
    categories.size match {
      case 0 => // Ignore
      case 1 => filter.append("category", categories.head)
      case _ => filter.append("category", new Document("$in", toJList(categories)))
    }
    query(filter, OrderByTime, callback)
  }

}
