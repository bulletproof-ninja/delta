package delta.read

private[read] trait StreamId {
  type Id
  protected type StreamId
  implicit protected def StreamId(id: Id): StreamId
}
