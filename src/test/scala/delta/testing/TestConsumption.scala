package delta.testing

import java.util.UUID
import java.util.concurrent.Executors

import delta._, MessageTransport.Topic
import delta.Channel
import delta.process._
import delta.util.TransientEventStore
import delta.util.LocalTransport
import delta.read.impl.PrebuiltReadModel

import scuff.LamportClock
import scuff.Codec

import scala.concurrent._, duration._

import scala.reflect.ClassTag
import delta.process.LiveProcessConfig.DelayedReplay
import scuff.JavaSerializer


class TestConsumption
extends BaseTest {

  implicit val Scheduler = Executors.newSingleThreadScheduledExecutor()

  object CharFmt extends EventFormat[Char, String] {

    protected def getName(cls: EventClass): String = {
      assert(cls == classOf[Character])
      "char"
    }
    protected def getVersion(cls: EventClass): Byte = 1

    def encode(evt: Char): String = evt.toString

    def decode(encoded: Encoded): Char = encoded.data charAt 0

  }

  def newUpdateTransport[S](topic: Topic): MessageTransport[(UUID, Update[S])] =
    new LocalTransport[(UUID, Update[S])](ec)

  def newEventStore[EVT](
      theTicker: Ticker, evtFmt: EventFormat[EVT, String])
      : EventStore[UUID, EVT]
      // with TransactionPublishing[UUID, EVT]
      =
    new TransientEventStore[UUID, EVT, String](ec, evtFmt) {
      def ticker = theTicker
    }

  def newProcessStore[S <: AnyRef: ClassTag](name: String)
      (implicit codec: Codec[S, String]): StreamProcessStore[UUID, S, S] =
    new InMemoryProcStore[UUID, S, S](name)


  test("recover from missing transactions during replay") {
    val ticker = LamportTicker(new LamportClock(99))
    val ch = Channel("4")
    val topic = Topic("update")
    val transport = newUpdateTransport[String](topic)
    val helloW = UUID.randomUUID()
    val es = newEventStore[Char](ticker, CharFmt)

    val helloWorld = "Hello, World!"
    val done =
      helloWorld.zipWithIndex.foldLeft(Future.unit: Future[Any]) {
        case (f, (char, idx)) =>
          f.flatMap { _ =>
            es.commit(ch, helloW, idx, char :: Nil, Map.empty)
          }
      }
    done.await
    val procStore = newProcessStore[String]("phrase-book")
    val updateHub = MessageHub.forUpdates[UUID, String](transport, topic)
    val rm =
      new PrebuiltReadModel[UUID, UUID, String]("test", 5000.millis)
      with read.SnapshotReaderSupport[UUID, String]
      with read.MessageSourceSupport[UUID, String, String] {
        protected def snapshotReader = procStore
        protected def updateSource = updateHub
    }

    procStore.write(helloW, Snapshot("Hello, W", 7, 107)).await

    val replayConfig = ReplayProcessConfig(
        writeBatchSize = 1, completionTimeout = 5.seconds, tickWindow = 1)

    val consumer = new IdempotentConsumer[UUID, Char, String, String] {
      protected def adHocExeCtx = ec
      protected def processStore = procStore
      override protected def tickWatermark: Option[Tick] =
        super.tickWatermark.map(_ + replayConfig.tickWindow + 2) // Too high, we'll miss txs on replay
      protected def selector(es: EventSource): es.Selector =
        es.ChannelSelector(ch)
      protected def process(tx: Transaction, currState: Option[String]): Future[String] =
        tx.events.foldLeft(new StringBuilder(currState getOrElse "")) {
          case (sb, char) => sb append char
        }.result()
      protected def reportFailure(th: Throwable): Unit = ec reportFailure th
      protected def onUpdate(id: UUID, update: Update): Unit =
        updateHub.publish(id, update)
    }

    val replayProcess = consumer.start(es,
      replayConfig, LiveProcessConfig(DelayedReplay(2.seconds, Scheduler)))

    val (replayResult, live) = replayProcess.finished.await

    assert(replayResult.processErrors.isEmpty)

    val snapshot = procStore.read(helloW).await.get
    assert(helloWorld.length-1 === snapshot.revision)
    assert(snapshot.revision + 100L === snapshot.tick)
    assert(helloWorld === snapshot.state)

    es.commit(ch, helloW, snapshot.revision + 1, '?' :: '!' :: Nil).await
    val updSnapshot = rm.read(helloW, snapshot.revision + 1).await
    assert(s"$helloWorld?!" === updSnapshot.state)

    live.cancel()

    assert(this.failures.isEmpty)
  }
}
