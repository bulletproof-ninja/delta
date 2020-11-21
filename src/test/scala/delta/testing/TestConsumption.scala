package delta.testing

import java.util.UUID
import java.util.concurrent.Executors

import delta._, MessageTransport.Topic
import delta.Channel
import delta.process._
import delta.util.TransientEventStore
import delta.util.LocalTransport
import delta.read.impl.SimplePrebuiltReadModel

import scuff.LamportClock
import scuff.Codec

import scala.concurrent._, duration._

import org.junit._, Assert._
import scala.reflect.ClassTag

object TestConsumption {
  val Scheduler = Executors.newSingleThreadScheduledExecutor()
  implicit def ec = RandomDelayExecutionContext
  object CharFmt extends EventFormat[Char, String] {

    protected def getName(cls: EventClass): String = {
      assert(cls == classOf[Character])
      "char"
    }
    protected def getVersion(cls: EventClass): Byte = 1

    def encode(evt: Char): String = evt.toString

    def decode(encoded: Encoded): Char = encoded.data charAt 0

  }
}

class TestConsumption {

  import TestConsumption._

  def newTransport[S](topic: Topic): MessageTransport { type TransportType = (UUID, Update[S]) } =
    new LocalTransport[(UUID, Update[S])](_ => topic, ec)

  def newEventStore[EVT](
      channels: Set[Channel], ticker: Ticker, evtFmt: EventFormat[EVT, String])
      : EventStore[UUID, EVT] with TransactionPublishing[UUID, EVT] =
    new TransientEventStore[UUID, EVT, String](ec, evtFmt)(_ => ticker)
    with LocalTransactionPublishing[UUID, EVT] {
      def txChannels = channels
    }

  def newProcessStore[S <: AnyRef: ClassTag](name: String)
      (implicit codec: Codec[S, String]): StreamProcessStore[UUID, S, S] =
    new InMemoryProcStore[UUID, S, S](name)


  @Test
  def `recover from missing transactions during replay`(): Unit = {
    val failures = new java.util.concurrent.CopyOnWriteArrayList[Throwable]
    val ticker = LamportTicker(new LamportClock(99), () => ())
    val ch = Channel("1")
    val topic = Topic("update")
    val transport = newTransport[String](topic)
    val helloW = UUID.randomUUID()
    val es = newEventStore[Char](Set(ch), ticker, CharFmt)

    val helloWorld = "Hello, World!"
    val done =
      helloWorld.zipWithIndex.foldLeft(Future.unit: Future[Any]) {
        case (f, (char, idx)) =>
          f.flatMap { _ =>
            es.commit(ch, helloW, idx, es.ticker.nextTick(), char :: Nil, Map.empty)
          }
      }
    done.await
    val procStore = newProcessStore[String]("phrase-book")
    val updateHub = MessageHub[UUID, String](transport, topic)
    val rm =
      new SimplePrebuiltReadModel[UUID, String, UUID](500.millis)
      with read.SnapshotReaderSupport[UUID, String]
      with read.MessageHubSupport[UUID, String, String] {
        protected def scheduler = Scheduler
        protected def snapshotReader = procStore
        protected def hub = updateHub
    }

    procStore.write(helloW, Snapshot("Hello, W", 7, 107)).await

    val replayConfig = ReplayProcessConfig(
        writeBatchSize = 1, finishProcessingTimeout = 5.seconds, tickWindow = 1)

    val consumer = new PersistentMonotonicConsumer[UUID, Char, String, String] {
      protected def adHocContext = ec
      protected def processStore = procStore
      override protected def tickWatermark: Option[Tick] =
        super.tickWatermark.map(_ + replayConfig.tickWindow + 2) // Too high, we'll miss txs on replay
      protected def selector(es: EventSource): es.Selector =
        es.ChannelSelector(ch)
      protected def process(tx: Transaction, currState: Option[String]): Future[String] =
        tx.events.foldLeft(new StringBuilder(currState getOrElse "")) {
          case (sb, char) => sb append char
        }.result()
      protected def reportFailure(th: Throwable): Unit = failures add th
      protected def onUpdate(id: UUID, update: Update): Unit =
        updateHub.publish(id, update)
    }

    val replayProcess = consumer.consume(es,
      replayConfig, LiveProcessConfig.apply(2.seconds, Scheduler))

    val (replayResult, live) = replayProcess.finished.await

    assert(replayResult.processErrors.isEmpty)

    val snapshot = procStore.read(helloW).await.get
    assertEquals(helloWorld.length-1, snapshot.revision)
    assertEquals(snapshot.revision + 100L, snapshot.tick)
    assertEquals(helloWorld, snapshot.state)

    es.commit(ch, helloW, snapshot.revision + 1, es.ticker.nextTick(), '?' :: '!' :: Nil, Map.empty).await
    val updSnapshot = rm.read(helloW, snapshot.revision + 1).await
    assertEquals(s"$helloWorld?!", updSnapshot.state)

    live.cancel()

    assert(failures.isEmpty())
  }
}
