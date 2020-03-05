package delta

import scuff.FakeType
import scala.util.control.NonFatal

@SerialVersionUID(1)
final case class Transaction[+ID, +EVT](
    tick: Long,
    channel: Transaction.Channel,
    stream: ID,
    revision: Int,
    metadata: Map[String, String],
    events: List[EVT]) {
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    Transaction.Serialization.writeObject(this, out)
  }
  private def readObject(inp: java.io.ObjectInputStream): Unit = {
    Transaction.Serialization.readObject[ID, EVT](inp) {
      case (tick, ch, id, rev, metadata, events) =>
        val surgeon = new scuff.reflect.Surgeon(this)
        surgeon.tick = tick
        surgeon.channel = ch
        surgeon.stream = id
        surgeon.revision = rev
        surgeon.metadata = metadata
        surgeon.events = events
        this
    }
  }

}

object Transaction {

  type Channel = Channel.Type
  val Channel: FakeType[String] { type Type <: AnyRef } = new FakeType[String] {
    type Type = String
    def apply(str: String) = str
  }

  object Serialization {
    def writeObject(tx: Transaction[_, _], out: java.io.ObjectOutput): Unit = {
      out.writeLong(tx.tick)
      out.writeUTF(tx.channel.toString)
      out.writeObject(tx.stream)
      out.writeInt(tx.revision)
      out.writeChar(tx.metadata.size)
      tx.metadata.foreach {
        case (key, value) =>
          out.writeUTF(key)
          out.writeUTF(value)
      }
      tx.events.reverse.foreach { evt =>
        try out writeObject evt catch {
          case NonFatal(cause) =>
            throw new RuntimeException(s"Failed to serialize event: ${evt.getClass.getName}", cause)
        }
      }
      out.writeObject(null)
    }
    @annotation.tailrec
    private def readEvents(
        in: java.io.ObjectInput,
        events: List[_ <: AnyRef] = Nil): List[_ <: AnyRef] =
      in.readObject match {
        case null => events
        case evt => readEvents(in, evt :: events)
      }
    def readObject[ID, EVT](
        inp: java.io.ObjectInput)(
        ctor: (Long, Channel, ID, Int, Map[String, String], List[EVT]) => Transaction[ID, EVT]): Transaction[ID, EVT] = {
      val tick = inp.readLong()
      val ch = Channel(inp.readUTF)
      val id = inp.readObject.asInstanceOf[ID]
      val rev = inp.readInt()
      val mdSize: Int = inp.readChar()
      var metadata = Map.empty[String, String]
      while (metadata.size < mdSize) {
        metadata += inp.readUTF -> inp.readUTF
      }
      val events = readEvents(inp).asInstanceOf[List[EVT]]
      ctor(tick, ch, id, rev, metadata, events)
    }
  }
}
