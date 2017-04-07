package delta.mongo

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.bson.Document
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.codecs.configuration.{ CodecRegistries, CodecRegistry }
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

import com.mongodb._
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.{ MongoClient, MongoCollection }
import com.mongodb.connection.ClusterType

import scuff.ScuffByte
import scuff.concurrent.{ StreamCallback, Threads }
import delta.EventCodec

object MongoEventStore {
  def getCollection(
    ns: MongoNamespace, client: MongoClient,
    codecs: Codec[_]*): MongoCollection[Document] = {
    if (codecs.isEmpty) getCollection(ns, client, DEFAULT_CODEC_REGISTRY)
    else getCollection(ns, client, CodecRegistries.fromCodecs(codecs: _*))
  }
  def getCollection(
    ns: MongoNamespace, client: MongoClient,
    optRegistry: CodecRegistry): MongoCollection[Document] = {
    val registry = optRegistry match {
      case null => CodecRegistries.fromRegistries(DEFAULT_CODEC_REGISTRY, client.getSettings.getCodecRegistry)
      case reg => CodecRegistries.fromRegistries(reg, DEFAULT_CODEC_REGISTRY, client.getSettings.getCodecRegistry)
    }
    val (rc, wc) = client.getSettings.getClusterSettings.getRequiredClusterType match {
      case ClusterType.REPLICA_SET => ReadConcern.MAJORITY -> WriteConcern.MAJORITY
      case _ => ReadConcern.DEFAULT -> WriteConcern.JOURNALED
    }
    client
      .getDatabase(ns.getDatabaseName)
      .getCollection(ns.getCollectionName)
      .withReadConcern(rc)
      .withWriteConcern(wc)
      .withCodecRegistry(registry)
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
abstract class MongoEventStore[ID: Codec, EVT, CH: Codec](
  dbColl: MongoCollection[Document])(implicit codec: EventCodec[EVT, Document])
    extends delta.EventStore[ID, EVT, CH] {

  protected val store = {
    val txnCodec = new TransactionCodec(
      dbColl.getCodecRegistry.get(classOf[Document]).ensuring(_ != null, "No Document codec found!"))
    val registry = CodecRegistries.fromRegistries(
      CodecRegistries.fromCodecs(
        implicitly[Codec[ID]],
        implicitly[Codec[CH]],
        txnCodec),
      dbColl.getCodecRegistry)

    val store = dbColl.withCodecRegistry(registry).withDocumentClass(classOf[TXN])
    withBlockingCallback[String]() {
      store.createIndex(new Document("_id.stream", 1).append("_id.rev", 1), _)
    }
    withBlockingCallback[String]() {
      store.createIndex(new Document("tick", 1), _)
    }
    withBlockingCallback[String]() {
      store.createIndex(new Document("channel", 1), _)
    }
    withBlockingCallback[String]() {
      store.createIndex(new Document("events.name", 1), _)
    }
    store
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
    }(Threads.PiggyBack) // map revision on the same thread
  }

  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit = {
    queryWith(new Document("_id.stream", stream), callback, OrderByRevision)
  }

  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[TXN]): Unit = {
    val filter = new Document("_id.stream", stream)
    if (fromRevision > 0) {
      filter.append("_id.rev", new Document("$gte", fromRevision))
    }
    queryWith(filter, callback, OrderByRevision)
  }
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
    queryWith(filter, callback, OrderByRevision)
  }

  def commit(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: List[EVT], metadata: Map[String, String]): Future[TXN] = {
    val txn = Transaction(tick, channel, stream, revision, metadata, events)
    val insertFuture = withFutureCallback[Void] { callback =>
      store.insertOne(txn, callback)
    }.map(_ => txn)(Threads.PiggyBack)
    insertFuture.recoverWith {
      case e: MongoWriteException if e.getError.getCategory == ErrorCategory.DUPLICATE_KEY =>
        withFutureCallback[TXN] { callback =>
          store.find(new Document("_id.stream", stream).append("_id.rev", revision))
            .limit(1)
            .first(callback)
        }.map(conflicting => throw new DuplicateRevisionException(conflicting.get))(Threads.PiggyBack)
    }(Threads.PiggyBack)
  }

  protected def queryWith(filter: Document, callback: StreamCallback[TXN], ordering: Document = null): Unit = {
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

  private class TransactionCodec(docCodec: Codec[Document])
      extends Codec[TXN] {

    import org.bson.{ BsonReader, BsonType, BsonWriter }
    implicit def tag2class[T](tag: ClassTag[T]): Class[T] =
      tag.runtimeClass.asInstanceOf[Class[T]]
    def getEncoderClass = classOf[TXN]
    private[this] val idCodec = implicitly[Codec[ID]]
    private[this] val chCodec = implicitly[Codec[CH]]

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
            writer.writeString("name", codec.name(evt))
            writer.writeInt32("v", codec.version(evt).unsigned)
            writer.writeName("data"); docCodec.encode(writer, codec.encode(evt), ctx)
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
    private def readEvents(channel: CH, reader: BsonReader, events: List[EVT] = Nil)(implicit ctx: DecoderContext): List[EVT] = {
      if (reader.readBsonType() == BsonType.END_OF_DOCUMENT) {
        events.reverse
      } else {
        val evt = readDocument(reader) {
          val name = reader.readString("name")
          val version = reader.readInt32("v").toByte
          reader.readName("data")
          val data = docCodec.decode(reader, ctx)
          codec.decode(name, version, data)
        }
        readEvents(channel, reader, evt :: events)
      }
    }
    def decode(reader: BsonReader, ctx: DecoderContext): TXN = {
        implicit def decCtx = ctx
      reader.readStartDocument()
      val (id, rev) = readDocument(reader, "_id") {
        reader.readName("stream")
        idCodec.decode(reader, ctx) -> reader.readInt32("rev")
      }
      val tick = reader.readInt64("tick")
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
      Transaction(tick, channel, id, rev, metadata, events)
    }

  }

  def maxTick(): Future[Option[Long]] = getFirst[Long]("tick", reverse = true)

  private def getFirst[T](name: String, reverse: Boolean): Future[Option[T]] = {
    withFutureCallback[Document] { callback =>
      store.find(new Document, classOf[Document])
        .projection(new Document(name, true).append("_id", false))
        .sort(new Document(name, if (reverse) -1 else 1))
        .limit(1)
        .first(callback)
    }.map { optDoc =>
      optDoc.map(_.get(name).asInstanceOf[T])
    }(Threads.PiggyBack) // map first on same thread
  }

  private def toJList[T](tr: Traversable[T]): java.util.List[T] = {
    tr.foldLeft(new java.util.ArrayList[T](8)) {
      case (list, t) =>
        list add t
        list
    }
  }

  private def toDoc(streamFilter: Selector, docFilter: Document = new Document): Document = {
    streamFilter match {
      case Everything => // Ignore
      case ChannelSelector(channels) =>
        docFilter.append("channel", new Document("$in", toJList(channels)))
      case EventSelector(byChannel) =>
        val matchByChannel = byChannel.toSeq.map {
          case (ch, eventTypes) =>
            val matcher = new Document("channel", ch)
            val evtNames = eventTypes.map(codec.name)
            matcher.append("events.name", new Document("$in", toJList(evtNames)))
        }
        if (matchByChannel.size == 1) {
          import collection.JavaConverters._
          matchByChannel.head.asScala.foreach { entry =>
            docFilter.append(entry._1, entry._2)
          }
        } else {
          docFilter.append("$or", toJList(matchByChannel))
        }
      case StreamSelector(id, _) =>
        docFilter.append("_id.stream", id)
    }
    docFilter
  }

  def query(streamFilter: Selector)(callback: StreamCallback[TXN]): Unit = {
    queryWith(toDoc(streamFilter), callback)
  }

  def querySince(sinceTick: Long, streamFilter: Selector)(callback: StreamCallback[TXN]): Unit = {
    val docFilter = new Document("tick", new Document("$gte", sinceTick))
    queryWith(toDoc(streamFilter, docFilter), callback)
  }

}
