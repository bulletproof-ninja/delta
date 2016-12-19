package sampler.mongo

import java.io.File
import java.sql.ResultSet

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Random, Success, Try }

import org.junit.{ Before, Test }
import org.junit.AfterClass
import org.junit.Assert._

import sampler.aggr._
import scuff._
import scuff.ddd.Repository
import ulysses.{ EventStore, LamportClock }
import ulysses.ddd.EntityRepository
import ulysses.util.LocalPublishing
import scuff.ddd.DuplicateIdException
import scuff.concurrent.{
  StreamCallback,
  StreamPromise
}
import ulysses.EventSource
import com.mongodb.async.client.MongoClient
import ulysses.mongo.MongoEventStore
import com.mongodb.async.SingleResultCallback
import scala.concurrent.Promise
import org.bson.codecs.configuration.CodecRegistry
import com.mongodb.MongoNamespace
import ulysses.EventCodec
import scuff.reflect.Surgeon
import sampler.MyDate

class TestSampler extends sampler.TestSampler {

  override val es = {
    import ulysses.mongo._
    import com.mongodb.async.client._
    val client = MongoClients.create()
    val ns = new MongoNamespace("unit-testing", "event-store")
    val txnCollection = MongoEventStore.getCollection(ns, client, AggrRootRegistry)
      implicit def aggrCodec = AggrRootRegistry.codec
      implicit def evtCdc = BsonDomainEventCodec
    new MongoEventStore[Int, DomainEvent, sampler.Aggr.Value](
      txnCollection) with LocalPublishing[Int, DomainEvent, sampler.Aggr.Value] {
      def publishCtx = global
    }
  }
}
