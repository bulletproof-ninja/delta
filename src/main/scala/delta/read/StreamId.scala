package delta.read

private[read] trait StreamId[ID] {
  protected type StreamId
  protected def StreamId(id: ID): StreamId
}
