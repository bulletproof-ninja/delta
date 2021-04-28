package delta.mongo

import org.bson._

class SnapshotCodec[S](
  stateCodec: scuff.Codec[S, BsonValue],
  stateFieldName: S => String = (_: S) => SnapshotCodec.DefaultStateFieldName)
extends scuff.Codec[delta.Snapshot[S], BsonDocument] {

  def this(
    stateCodec: scuff.Codec[S, BsonValue],
    stateFieldName: String) =
    this(stateCodec, _ => stateFieldName)

  type Snapshot = delta.Snapshot[S]

  def toDoc(snapshot: Snapshot, document: BsonDocument = new BsonDocument): BsonDocument = {
      document
        .append("revision", new BsonInt32(snapshot.revision))
        .append("tick", new BsonInt64(snapshot.tick))
      (stateCodec encode snapshot.state) match {
        case _: BsonNull | _: BsonUndefined => document
        case bsonState => document.append(stateFieldName(snapshot.state), bsonState)
      }
  }

  def encode(snapshot: Snapshot): BsonDocument =
    toDoc(snapshot)

  def decode(doc: BsonDocument): Snapshot = {
    val revision = doc.remove("revision").asInt32.intValue
    val tick = doc.remove("tick").asInt64.longValue
    val state: S = stateCodec decode {
      doc.size match {
        case 0 =>
          BsonNull.VALUE
        case size =>
          val field = if (size == 1) doc.getFirstKey else SnapshotCodec.DefaultStateFieldName
          doc.get(field) match {
            case null => sys.error(s"Snapshot document is has ambiguous state: ${doc.keySet}")
            case bson => bson
          }
      }
    }
    new Snapshot(state, revision, tick)
  }

}

object SnapshotCodec {

  def DefaultStateFieldName = "data"

  def apply[S](
      stateFieldName: S => String)(
      stateCodec: scuff.Codec[S, BsonValue]): SnapshotCodec[S] =
    new SnapshotCodec(stateCodec, stateFieldName)

  def apply[S](
      stateFieldName: String = DefaultStateFieldName)(
      stateCodec: scuff.Codec[S, BsonValue]): SnapshotCodec[S] =
    new SnapshotCodec(stateCodec, stateFieldName)

  def apply[S](
      stateCodec: scuff.Codec[S, BsonValue])(
      stateFieldName: S => String): SnapshotCodec[S] =
    new SnapshotCodec(stateCodec, stateFieldName)


}
