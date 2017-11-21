package tech.sourced.engine

import org.scalatest._

class NoMetadataSpec extends FlatSpec with Matchers with BaseSivaSpec with BaseSparkSpec {

  var engine: Engine = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    engine = Engine(ss, resourcePath)
  }

  "fromMetadata" should "load repositories from metadata" in {
    engine.getRepositories.filter("is_fork = false").count() //should be(4)
  }

  it should "load references from metadata" in {
    engine.getRepositories.getReferences.count() //should be(56)
    engine.getRepositories.getMaster.count()// should be(5)
  }

  it should "load commits from metadata" in {
    engine.getRepositories
      .getReferences
      .getCommits
      .count()// should be(4444)

    engine.getRepositories
      .getReferences
      .getCommits
      .getFirstReferenceCommit
      .count() //should be(54)
  }

  it should "load tree entries from metadata" in {
    engine.getRepositories
      .getReferences
      .getCommits
      .getFirstReferenceCommit
      .getTreeEntries
      .count()// should be(4450)
  }

  it should "load blobs from repositories, but entries from metadata" in {
    engine.getRepositories
      .getReferences
      .getCommits
      .getFirstReferenceCommit
      .getTreeEntries
      .getBlobs
      .count()// should be(4419)
  }
}
