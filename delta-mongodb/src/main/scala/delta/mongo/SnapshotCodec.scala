package delta.mongo

import org.bson._

class SnapshotCodec[S](
  stateCodec: scuff.Codec[S, BsonValue],
  stateFieldName: String = SnapshotCodec.DefaultStateFieldName)
extends scuff.Codec[delta.Snapshot[S], BsonDocument] {

  type Snapshot = delta.Snapshot[S]

  def toDoc(snapshot: Snapshot, document: BsonDocument = new BsonDocument): BsonDocument = {
      document
        .append("revision", new BsonInt32(snapshot.revision))
        .append("tick", new BsonInt64(snapshot.tick))
      (stateCodec encode snapshot.state) match {
        case _: BsonNull | _: BsonUndefined => document
        case bsonState => document.append(stateFieldName, bsonState)
      }
  }

  def encode(snapshot: Snapshot): BsonDocument =
    toDoc(snapshot)

  def decode(doc: BsonDocument): Snapshot = {
    val revision = doc.getInt32("revision", -1).intValue
    val tick = doc.getInt64("tick").longValue
    val state: S = stateCodec decode {
      doc.get(stateFieldName) match {
        case null | _: BsonNull | _: BsonUndefined => BsonNull.VALUE
        case bson => bson
      }
    }
    new Snapshot(state, revision, tick)
  }

}

object SnapshotCodec {

  def DefaultStateFieldName = "state"

  def apply[S](
      stateFieldName: String = DefaultStateFieldName)(
      stateCodec: scuff.Codec[S, BsonValue]): SnapshotCodec[S] =
    new SnapshotCodec(stateCodec, stateFieldName)

}
