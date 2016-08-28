package ulysses

class StreamProcessor[ID, EVT, CH] {
  type TXN = Transaction[ID, EVT, CH]
  sealed trait Consumer {
    def consume(txn: TXN): Unit
  }
  trait ReplayConsumer extends Consumer {
    def replayFinished(): LiveConsumer
    trait LiveConsumer extends Consumer {
      def expectedRevision(id: ID): Int
    }
  }
}
