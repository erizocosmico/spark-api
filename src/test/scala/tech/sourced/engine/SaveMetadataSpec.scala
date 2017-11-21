package tech.sourced.engine

import org.scalatest._

class SaveMetadataSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath)
  }

  "saveMetadataTo" should "save all metadata" in {
    engine.saveMetadataTo("jdbc:postgresql://0.0.0.0:5432/testing", Map(
      "user" -> "testing", "password" -> "testing", "sslmode" -> "disable",
      "driver" -> "org.postgresql.Driver"
    ), table => s"${table}_metadata")
  }

}
