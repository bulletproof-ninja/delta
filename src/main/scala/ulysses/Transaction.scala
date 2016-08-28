package ulysses

@SerialVersionUID(1)
final case class Transaction[ID, EVT, CH] private[ulysses] (
    tick: Long,
    channel: CH,
    stream: ID,
    revision: Int,
    metadata: Map[String, String],
    events: Vector[EVT]) {
  private def writeObject(out: java.io.ObjectOutputStream) {
    out.writeLong(this.tick)
    out.writeObject(this.channel)
    out.writeObject(this.stream)
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
  private def readEvents(
    in: java.io.ObjectInputStream,
    events: Vector[_ <: AnyRef] = Vector.empty): Vector[_ <: AnyRef] =
    in.readObject match {
      case null => events
      case evt => readEvents(in, events :+ evt)
    }
  private def readObject(in: java.io.ObjectInputStream) {
    val surgeon = new scuff.reflect.Surgeon(this)
    surgeon.set('tick, in.readLong)
    surgeon.set('channel, in.readObject)
    surgeon.set('stream, in.readObject)
    surgeon.set('revision, in.readInt)
    val mapSize: Int = in.readChar
    var metadata = Map.empty[String, String]
    while (metadata.size < mapSize) {
      metadata += in.readUTF -> in.readUTF
    }
    surgeon.set('metadata, metadata)
    surgeon.set('events, readEvents(in))
  }

}
