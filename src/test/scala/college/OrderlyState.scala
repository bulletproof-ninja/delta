//package college
//
//import delta.Transaction
//
//object SafeModel {
//  def apply(builder: ModelBuilder[M, TXN],
//            model: M, revision: Int): SafeModel[M, TXN] = {
//    require(model != null, "Model cannot be null")
//    require(revision >= 0, s"Revision cannot be negative: $revision")
//    new SafeModel(model, revision, Map.empty, builder)
//  }
//  def apply(builder: ModelBuilder[M, TXN]): SafeModel[M, TXN] = {
//    new SafeModel(null, -1, Map.empty, builder)
//  }
//}
//
//final class SafeModel[M, TXN] private (
//    val model: M,
//    val revision: Int,
//    futureRevisions: Map[Int, TXN],
//    builder: ModelBuilder[M, TXN]) {
//
//  def apply(txn: TXN): Option[this.type] = {
//    val expectedRevision = revision + 1
//    if (txn.revision == expectedRevision) {
//      if (state == null)
//      val updated = process(txn)
//      if (futureRevisions.contains(txn.revision + 1)) {
//        val nextTxn = futureRevisions(txn.revision + 1)
//        update(futureRevisions - nextTxn.revision).apply(nextTxn)
//      } else {
//        assert(updated.revision == expectedRevision)
//        Some(updated)
//      }
//    } else if (txn.revision > expectedRevision) {
//      update(futureRevisions.updated(txn.revision, txn))
//      None
//    } else None
//  }
//}
//
//trait ModelBuilder[M, TXN] {
//  def first(txn: TXN): M
//  def next(model: M, txn: TXN): M
//}
