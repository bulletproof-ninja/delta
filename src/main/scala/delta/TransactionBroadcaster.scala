// package delta

// /**
//   * Distributed transcation hub.
//   */
// class TransactionHub[ID, EVT](
//   txHub: MessageHub[ID, Transaction[ID, EVT]])
// extends MessageSource[ID, Transaction[ID, EVT]] {

//   type Transaction = delta.Transaction[ID, EVT]

//   val sink: Transaction => Unit =
//     tx => txHub.publish(tx.stream, tx)



// }
