package ulysses.hazelcast

import com.hazelcast.core._
import ulysses.Publishing
import scala.util.control.NonFatal
import scuff.Subscription
import scala.util.Try
import ulysses.EventCodec
import concurrent.blocking

trait TopicPublishing[ID, EVT, CH] extends Publishing[ID, EVT, CH] {

  protected def allChannels: Set[CH]
  protected def getTopic(ch: CH): ITopic[TXN]

  protected def publish(txn: TXN): Unit = blocking {
    getTopic(txn.channel).publish(txn)
  }

  private class Subscriber(selector: Selector, callback: TXN => Unit)
      extends MessageListener[TXN] {
    def onMessage(msg: Message[TXN]): Unit = {
      val txn = msg.getMessageObject
      if (selector.include(txn)) callback(txn)
    }
  }

  private def subscribe(channels: Iterable[CH], selector: Selector, callback: TXN => Unit): List[Subscription] = {
    channels.toList.map { ch =>
      val topic = getTopic(ch)
      val regId = topic addMessageListener new Subscriber(selector, callback)
      new Subscription {
        def cancel() = topic removeMessageListener regId
      }
    }
  }

  def subscribe(selector: Selector)(
    callback: TXN => Unit): Subscription = {
    val subscriptions = selector match {
      case Everything => subscribe(allChannels, selector, callback)
      case ChannelSelector(channels) => subscribe(channels, selector, callback)
      case EventSelector(byChannel) => subscribe(byChannel.keys, selector, callback)
      case StreamSelector(id, channel) => subscribe(List(channel), selector, callback)
    }
    new Subscription {
      def cancel() = subscriptions.foreach(s => Try(s.cancel))
    }
  }

}
