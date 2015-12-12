package ulysses

import java.io.InvalidObjectException

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.control.NoStackTrace

import scuff.concurrent.StreamCallback

/**
 * Event store.
 * @tparam ID Id type
 * @tparam EVT Event type
 * @tparam CAT Category type
 */
trait EventStore[ID, EVT, CAT] extends EventSource[ID, EVT, CAT] {

  protected def Transaction(
    clock: Long,
    category: CAT,
    streamId: ID,
    revision: Int,
    metadata: Map[String, String],
    events: Seq[EVT]) = new Transaction(clock, category, streamId, revision, metadata, events)

  final class DuplicateRevisionException(val conflictingTransaction: this.Transaction)
    extends RuntimeException(s"Revision ${conflictingTransaction.revision} already exists: ${conflictingTransaction.streamId}")
    with NoStackTrace

  /**
   * Create stream.
   * @param streamId Event stream identifier.
   * @param category The category, which the stream belongs to.
   * @param clock The clock used for global ordering, if applicable.
   * @param events The events, at least one.
   * @return Potential [[DuplicateRevisionException]] if the stream already exists.
   */
  def createStream(streamId: ID, category: CAT, clock: Long, events: Seq[EVT], metadata: Map[String, String] = Map.empty): Future[Transaction]
  /**
   * Append stream.
   * @param streamId Event stream identifier.
   * @param revision Event stream revision, which is expected to be committed. Must always equal previous revision + 1, thus must always be > 0.
   * @param clock The clock used for global ordering, if applicable.
   * @param events The events, at least one.
   * @return Potential [[DuplicateRevisionException]] if the expected revision has already been committed.
   * This indicates a race condition. Try, try again.
   */
  def appendStream(streamId: ID, revision: Int, clock: Long, events: Seq[EVT], metadata: Map[String, String] = Map.empty): Future[Transaction]

}
