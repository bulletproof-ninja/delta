package ulysses.mongo

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.{ ClassTag, classTag }
import scala.util.Success
import scala.util.control.NonFatal
import org.bson.{ BsonReader, BsonType, BsonWriter, Document }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import com.mongodb.{ Block, MongoNamespace, WriteConcern }
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.{ MongoClient, MongoCollection }
import com.mongodb.connection.ClusterType
import scuff.concurrent.StreamCallback
import scala.collection.immutable.Seq
import scala.collection.{ Seq => aSeq, Map => aMap }
import org.bson.codecs.configuration.CodecRegistry
import ulysses.EventCodec
import org.bson.codecs.BsonArrayCodec
import org.bson.BsonArray
import scala.concurrent.ExecutionContext
import com.mongodb.MongoWriteException
import com.mongodb.ErrorCategory
import ulysses.StreamFilter

object MongoEventStore {

  def getCollection(ns: MongoNamespace, client: MongoClient): MongoCollection[Document] = {
    val clusterType = client.getSettings.getClusterSettings.getRequiredClusterType
    val wc = if (clusterType == ClusterType.REPLICA_SET) WriteConcern.MAJORITY else WriteConcern.JOURNALED
    client.getDatabase(ns.getDatabaseName).getCollection(ns.getCollectionName).withWriteConcern(wc)
  }

}

/**
  * Stores events, using this format:
  * {{{
  *   {
  *     _id: { // Indexed
  *       stream: 34534, // Stream identifier
  *       rev: 11, // Stream revision
  *     }
  *     tick: 1426320727122, // Clock tick.
  *     channel: "FooBar", // App specific type
  *     events: [{
  *      name: "MyEvent",
  *      v: 1, // Event version
  *      data: {} // App specific
  *     }]
  *   }
  * }}}
  */
class MongoEventStore[ID: Codec, EVT, CH: Codec](
  dbColl: MongoCollection[Document])(implicit codec: EventCodec[EVT, Document],
                                     protected val exeCtx: ExecutionContext)
    extends ulysses.EventStore[ID, EVT, CH] {

  protected val store = {
    withBlockingCallback[String]()(dbColl.createIndex(new Document("_id.stream", 1).append("_id.rev", 1), _))
    dbColl.withCodecRegistry(Factory.newCodecRegistry(dbColl.getCodecRegistry)).withDocumentClass(classOf[TXN])
  }

  private[this] val OrderByRevision = new Document("_id.rev", 1)

  def currRevision(stream: ID): Future[Option[Int]] = {
    withFutureCallback[Document] { callback =>
      store.find(new Document("_id.stream", stream), classOf[Document])
        .projection(new Document("_id.rev", true))
        .sort(new Document("_id.rev", -1))
        .limit(1)
        .first(callback)
    }.map { optDoc =>
      optDoc.map { doc =>
        doc.get("_id", classOf[Document]).getInteger("rev").intValue
      }
    }
  }

  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit = {
    query(new Document("_id.stream", stream), OrderByRevision, callback)
  }

  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[TXN]): Unit = {
    val filter = new Document("_id.stream", stream)
    if (fromRevision > 0) {
      filter.append("_id.rev", new Document("$gte", fromRevision))
    }
    query(filter, OrderByRevision, callback)
  }
  //  def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[TXN]): Unit = {
  //    val filter = new Document("_id.stream", stream).append("_id.rev", new Document("$lte", toRevision))
  //    query(filter, OrderByRevision, callback)
  //  }
  def replayStreamRange(stream: ID, revisionRange: collection.immutable.Range)(callback: StreamCallback[TXN]): Unit = {
    require(revisionRange.step == 1, s"Revision range must step by 1 only, not ${revisionRange.step}")
    val filter = new Document("_id.stream", stream)
    val from = revisionRange.head
    val to = revisionRange.last
    if (from == to) {
      filter.append("_id.rev", from)
    } else if (from == 0) {
      filter.append("_id.rev", new Document("$lte", to))
    } else {
      val range = new Document("$gte", from).append("$lte", to)
      filter.append("_id.rev", range)
    }
    query(filter, OrderByRevision, callback)
  }

  def commit(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: aSeq[EVT], metadata: aMap[String, String]): Future[TXN] = {
    val txn = Transaction(tick, channel, stream, revision, metadata, events)
    val insertFuture = withFutureCallback[Void] { callback =>
      store.insertOne(txn, callback)
    }.map(_ => txn)
    insertFuture.recoverWith {
      case e: MongoWriteException if e.getError.getCategory == ErrorCategory.DUPLICATE_KEY =>
        withFutureCallback[TXN] { callback =>
          store.find(new Document("_id.stream", stream).append("_id.rev", revision))
            .limit(1)
            .first(callback)
        }.map(conflicting => throw new DuplicateRevisionException(conflicting.get))
    }
  }

  protected def query(filter: Document, ordering: Document, callback: StreamCallback[TXN]): Unit = {
    val onTxn = new Block[TXN] {
      def apply(txn: TXN) = callback.onNext(txn)
    }
    val onFinish = new SingleResultCallback[Void] {
      def onResult(result: Void, t: Throwable) {
        if (t != null) callback.onError(t)
        else callback.onCompleted()
      }
    }
    store.find(filter).sort(ordering).forEach(onTxn, onFinish)
  }

  private object Factory {
    def newCodecRegistry(registry: CodecRegistry): org.bson.codecs.configuration.CodecRegistry = new org.bson.codecs.configuration.CodecRegistry {
      def get[C](cls: Class[C]): Codec[C] = {
        if (cls == classOf[TXN]) transactionCodec.asInstanceOf[Codec[C]]
        else registry.get(cls)
      }
      private[this] val docCodec: Codec[Document] = registry.get(classOf[Document])
      private[this] val transactionCodec = new org.bson.codecs.Codec[TXN] {
        import language.implicitConversions
        implicit def tag2class[T](tag: ClassTag[T]): Class[T] = tag.runtimeClass.asInstanceOf[Class[T]]
        def getEncoderClass = classOf[TXN]
        val idCodec = implicitly[Codec[ID]]
        val chCodec = implicitly[Codec[CH]]

        private def writeEntry(name: String, value: AnyRef, writer: BsonWriter)(implicit ctx: EncoderContext) {
          writer.writeName(name)
          value match {
            case doc: Document =>
              docCodec.encode(writer, doc, ctx)
            case _ =>
              registry.get(value.getClass.asInstanceOf[Class[AnyRef]]).encode(writer, value, ctx)
          }
        }
        private def writeDocument(writer: BsonWriter, name: String = null)(thunk: => Unit) {
          if (name != null) writer.writeStartDocument(name) else writer.writeStartDocument()
          thunk
          writer.writeEndDocument()
        }
        private def writeArray(name: String, writer: BsonWriter)(thunk: => Unit) {
          writer.writeStartArray(name)
          thunk
          writer.writeEndArray()
        }
        def encode(writer: BsonWriter, txn: TXN, ctx: EncoderContext) {
            implicit def encCtx = ctx
          writer.writeStartDocument()
          writeDocument(writer, "_id") {
            writer.writeName("stream"); idCodec.encode(writer, txn.stream, ctx)
            writer.writeInt32("rev", txn.revision)
          }
          writer.writeInt64("tick", txn.tick)
          writer.writeName("channel"); chCodec.encode(writer, txn.channel, ctx)
          if (txn.metadata.nonEmpty) {
            writeDocument(writer, "metadata") {
              txn.metadata.foreach {
                case (key, value) =>
                  writer.writeString(key, value)
              }
            }
          }
          writeArray("events", writer) {
            txn.events.foreach { evt =>
              writeDocument(writer) {
                writeEntry("name", codec.name(evt), writer)
                writeEntry("v", Integer.valueOf(codec.version(evt)), writer)
                val data = codec.encode(evt)
                writeEntry("data", data, writer)
              }
            }
          }
          writer.writeEndDocument()
        }
        private def readDocument[R](reader: BsonReader, name: String = null)(thunk: => R): R = {
          if (name != null) reader.readName(name)
          reader.readStartDocument()
          val r = thunk
          reader.readEndDocument()
          r
        }
        private def readArray[R](reader: BsonReader, name: String = null)(thunk: => R): R = {
          if (name != null) reader.readName(name)
          reader.readStartArray()
          val r = thunk
          reader.readEndArray()
          r
        }
        @annotation.tailrec
        private def readMetadata(reader: BsonReader, map: Map[String, String] = Map.empty): Map[String, String] = {
          if (reader.readBsonType() == BsonType.END_OF_DOCUMENT) {
            map
          } else {
            val name = reader.readName
            val value = reader.readString
            readMetadata(reader, map.updated(name, value))
          }
        }

        @annotation.tailrec
        private def readEvents(channel: CH, reader: BsonReader, events: Vector[EVT] = Vector.empty[EVT])(implicit ctx: DecoderContext): Vector[EVT] = {
          if (reader.readBsonType() == BsonType.END_OF_DOCUMENT) {
            events
          } else {
            val evt = readDocument(reader) {
              val name = reader.readString("name")
              val version = reader.readInt32("v").toShort
              reader.readName("data")
              val data = docCodec.decode(reader, ctx)
              codec.decode(name, version, data)
            }
            readEvents(channel, reader, events :+ evt)
          }
        }
        def decode(reader: BsonReader, ctx: DecoderContext): TXN = {
            implicit def decCtx = ctx
          reader.readStartDocument()
          val (id: ID, rev: Int) = readDocument(reader, "_id") {
            reader.readName("stream")
            idCodec.decode(reader, ctx) -> reader.readInt32("rev")
          }
          val clock = reader.readInt64("tick")
          val channel = {
            reader.readName("channel")
            chCodec.decode(reader, ctx)
          }
          val (metadata, events) = reader.readName match {
            case "metadata" =>
              val metadata = readDocument(reader)(readMetadata(reader))
              metadata -> readArray(reader, "events") {
                readEvents(channel, reader)
              }
            case "events" =>
              Map.empty[String, String] -> readArray(reader) {
                readEvents(channel, reader)
              }
            case other => throw new IllegalStateException(s"Unknown field: $other")
          }
          reader.readEndDocument()
          Transaction(clock, channel, id, rev, metadata, events)
        }
      }

    }
  }

  withBlockingCallback[String]()(store.createIndex(new Document("tick", 1), _))
  withBlockingCallback[String]()(store.createIndex(new Document("channel", 1), _))

  private[this] val OrderByTime = new Document("tick", 1)

  def lastTick(): Future[Option[Long]] = getFirst[Long]("tick", false)

  private def getFirst[T](name: String, desc: Boolean): Future[Option[T]] = {
    withFutureCallback[Document] { callback =>
      store.find(new Document, classOf[Document])
        .projection(new Document(name, true).append("_id", false))
        .sort(new Document(name, if (desc) 1 else -1))
        .limit(1)
        .first(callback)
    }.map { optDoc =>
      optDoc.map(_.get(name).asInstanceOf[T])
    }
  }

  private def toJList[T](tr: Traversable[T]): java.util.List[T] = {
    tr.foldLeft(new java.util.ArrayList[T](tr.size)) {
      case (list, t) =>
        list add t
        list
    }
  }

  private def toDoc(streamFilter: StreamFilter[ID, EVT, CH], docFilter: Document = new Document): Document = {
    import StreamFilter._
    streamFilter match {
      case Everything() => // Ignore
      case ByChannel(channels) =>
        docFilter.append("channel", new Document("$in", toJList(channels)))
      case ByEvent(evtTypes) =>

        val channels = evtTypes.keys
        docFilter.append("channel", new Document("$in", toJList(channels)))
        val evtNames = evtTypes.values.flatten.map(codec.name)
        docFilter.append("events.name", new Document("$in", toJList(evtNames)))
      case ByStream(id, _) =>
        docFilter.append("_id.stream", id)
    }
    docFilter
  }

  def replay(streamFilter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit = {
    query(toDoc(streamFilter), OrderByTime, callback)
  }

  def replaySince(sinceTick: Long, streamFilter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit = {
    val docFilter = new Document("tick", new Document("$gte", sinceTick))
    query(toDoc(streamFilter, docFilter), OrderByTime, callback)
  }

}
