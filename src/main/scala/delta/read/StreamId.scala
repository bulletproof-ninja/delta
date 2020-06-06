package delta.read

private[read] trait StreamId {
  type Id
  protected type StreamId
  protected def StreamId(id: Id): StreamId
}
