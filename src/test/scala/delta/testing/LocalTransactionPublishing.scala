// package delta.testing

// import delta.MessageTransportPublishing
// import delta.util.LocalTransport
// import scuff.Codec

// trait LocalTransactionPublishing[SID, EVT]
// extends MessageTransportPublishing[SID, EVT] {
//   def toTopic(ch: Channel) = Topic(s"transactions/$ch")
//   val txTransport = new LocalTransport[Transaction](RandomDelayExecutionContext)
//   val txTransportCodec = Codec.noop[Transaction]
// }
