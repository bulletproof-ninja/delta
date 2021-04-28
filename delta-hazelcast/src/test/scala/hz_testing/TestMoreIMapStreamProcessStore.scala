package hz_testing

import delta.process._
import delta.hazelcast._

import delta.testing._
import TestMoreStreamProcessStore._

import java.{util => ju}

import scuff.EmailAddress

import com.hazelcast.core.IMap
import com.hazelcast.query.extractor._
import com.hazelcast.projection.Projection

class EmailExtractor
extends ValueExtractor[delta.Snapshot[Contact], Void] {
  def extract(
      snapshot: delta.Snapshot[Contact], arg: Void, vc: ValueCollector[_]): Unit =
    vc.asInstanceOf[ValueCollector[Object]] addObject snapshot.state.email
}
class NumExtractor
extends ValueExtractor[delta.Snapshot[Contact], Void] {
  def extract(
      snapshot: delta.Snapshot[Contact], arg: Void, vc: ValueCollector[_]): Unit =
    vc.asInstanceOf[ValueCollector[Object]] addObject Int.box(snapshot.state.num)
}

object TestMoreIMapStreamProcessStore {
  import com.hazelcast.Scala._

  final def ContactEmailRef = "contact_email"
  final def ContactNumRef = "contact_num"

  val member = {
    import com.hazelcast.config._
    val conf = new Config

    conf.getNetworkConfig.getJoin.getMulticastConfig setEnabled false

    conf.getMapConfig("contacts-*")
      .addMapAttributeConfig(new MapAttributeConfig(ContactEmailRef, classOf[EmailExtractor].getName))
      .addMapAttributeConfig(new MapAttributeConfig(ContactNumRef, classOf[NumExtractor].getName))
      .addMapIndexConfig(new MapIndexConfig(ContactEmailRef, /* ordered = */ false))
      .addMapIndexConfig(new MapIndexConfig(ContactNumRef, /* ordered = */ false))

    conf.newInstance()
  }

  val client = {
    import com.hazelcast.client.config._
    import com.hazelcast.Scala.client._
    val conf = new ClientConfig
    conf.newClient()
  }

}

class TestMoreIMapStreamProcessStore
extends TestMoreStreamProcessStore {

  import TestMoreIMapStreamProcessStore._

  override def afterAll(): Unit = {
    client.shutdown()
    member.shutdown()
  }

  abstract class AbstractStore protected (
    protected val imap: IMap[Long, Snapshot])
  extends IMapStreamProcessStore[Long, Contact, Contact](
      imap, client.getLoggingService.getLogger(getClass)) {
    def tickWatermark: Option[Long] = None
    protected def emailRef: String = ContactEmailRef
    protected def blockingCtx = ec
  }


  class LookupStore(
    imap: IMap[Long, Snapshot])
  extends AbstractStore(imap)
  with IMapValueQueries[Long, Contact, Contact]
  with Lookup {
    protected def numRef: String = ContactNumRef
    protected def num2qry(num: Int) = num
    protected def email2qry(email: EmailAddress) = email
  }

  class DupeStore(
    imap: IMap[Long, Snapshot])
  extends AbstractStore(imap)
  with IMapAggregationSupport[Long, Contact, Contact]
  with DupeFinder {
    val getEmail =  new Projection[Contact, EmailAddress] {
      def transform(input: Contact): EmailAddress = input.email
    }
  }

  private def randomMapName: String = s"contacts-${ju.UUID.randomUUID}"

  override def newLookupStore() =
    new LookupStore(client.getMap[Long, Snapshot](randomMapName))
  override def newDupeStore() =
    new DupeStore(client.getMap[Long, Snapshot](randomMapName))

}
