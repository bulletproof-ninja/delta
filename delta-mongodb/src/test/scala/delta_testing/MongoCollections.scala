package delta_testing

import org.bson._
import com.mongodb._

import delta.testing.BaseTest

trait MongoCollections {
  base: BaseTest =>

  import com.mongodb.async.client._

  protected lazy val settings =
    com.mongodb.MongoClientSettings.builder()
      .uuidRepresentation(UuidRepresentation.STANDARD)
      .build()
  protected lazy val client =
    MongoClients create settings
  protected lazy val ns =
    new MongoNamespace(
      s"delta-testing-$instance",
      getClass.getSimpleName.replaceAll("[\\.\\$]+", "_"))

  protected def getCollection(name: String) =
    client
      .getDatabase(ns.getDatabaseName)
      .getCollection(ns.getCollectionName concat "-" concat name)
      .withDocumentClass(classOf[BsonDocument])

}
