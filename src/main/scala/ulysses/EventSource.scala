package ulysses

import scala.concurrent.Future
import scuff.concurrent.StreamCallback
import java.io.InvalidObjectException
import scala.collection.immutable.Seq

/**
 * Event source.
 */
trait EventSource[ID, EVT, CAT] {

  @SerialVersionUID(1)
  final case class Transaction private[ulysses] (
      clock: Long,
      category: CAT,
      streamId: ID,
      revision: Int,
      metadata: Map[String, String],
      events: Seq[EVT]) {
    private def writeObject(out: java.io.ObjectOutputStream) {
      out.writeLong(this.clock)
      out.writeObject(this.category)
      out.writeObject(this.streamId)
      out.writeInt(this.revision)
      out.writeChar(this.metadata.size)
      this.metadata.foreach {
        case (key, value) =>
          out.writeUTF(key)
          out.writeUTF(value)
      }
      this.events.foreach(out.writeObject)
      out.writeObject(null)
    }
    @annotation.tailrec
    private def readEvents(in: java.io.ObjectInputStream, events: Seq[EVT] = Vector.empty[EVT]): Seq[EVT] = in.readObject match {
      case null => events
      case evt => readEvents(in, events :+ evt.asInstanceOf[EVT])
    }
    private def readObject(in: java.io.ObjectInputStream) {
      val surgeon = new scuff.reflect.Surgeon(this)
      surgeon.set('clock, in.readLong)
      surgeon.set('category, in.readObject)
      surgeon.set('streamId, in.readObject)
      surgeon.set('revision, in.readInt)
      val mapSize: Int = in.readChar
      var metadata = Map.empty[String, String]
      while (metadata.size < mapSize) {
        metadata += in.readUTF -> in.readUTF
      }
      surgeon.set('metadata, metadata)
      surgeon.set('events, readEvents(in))
    }

    private def readObjectNoData(): Unit = throw new InvalidObjectException("Stream data required")
  }

  def currRevision(stream: ID): Future[Option[Int]]

  def replayStream(stream: ID)(callback: StreamCallback[Transaction]): Unit
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[Transaction]): Unit
  def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[Transaction]): Unit
  def replayStreamRange(stream: ID, revisionRange: collection.immutable.Range)(callback: StreamCallback[Transaction]): Unit

}
