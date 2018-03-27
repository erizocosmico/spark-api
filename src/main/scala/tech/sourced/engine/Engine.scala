package tech.sourced.engine

import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions.asScalaBuffer

/**
  * Engine is the main entry point to all usage of the source{d} spark-engine.
  * It has methods to configure all possible configurable options as well as
  * the available methods to start analysing repositories of code.
  *
  * {{{
  * import tech.sourced.engine._
  *
  * val engine = Engine(sparkSession, "/path/to/repositories")
  * }}}
  *
  * NOTE: Keep in mind that you will need to register the UDFs in the session
  * manually if you choose to instantiate this class directly instead of using
  * the companion object.
  *
  * {{{
  * import tech.sourced.engine.{Engine, SessionFunctions}
  *
  * engine = new Engine(sparkSession)
  * sparkSession.registerUDFs()
  * }}}
  *
  * The only method available as of now is getRepositories, which will generate
  * a DataFrame of repositories, which is the very first thing you need to
  * analyse repositories of code.
  *
  * @constructor creates a Engine instance with the given Spark session.
  * @param session Spark session to be used
  */
class Engine(val session: SparkSession,
             dbUrl: String,
             dbUser: String,
             dbPass: String) extends Logging {

  UserMetricsSystem.initialize(session.sparkContext, "Engine")

  session.conf.set(DBUrlKey, dbUrl)
  session.conf.set(DBUserKey, dbUser)
  session.conf.set(DBPasswordKey, dbPass)
  session.registerUDFs()

  /**
    * Returns a DataFrame with the data about the repositories found at
    * the specified repositories path in the form of siva files.
    * To call this method you need to have set before the repositories path,
    * you can do so by calling setRepositoriesPath or, preferably, instantiating
    * the Engine using the companion object.
    *
    * {{{
    * val reposDf = engine.getRepositories
    * }}}
    *
    * @return DataFrame
    */
  def getRepositories: DataFrame = getDataSource("repositories", session)

  /**
    * Retrieves the blobs of a list of repositories, reference names and commit hashes.
    * So the result will be a [[org.apache.spark.sql.DataFrame]] of all the blobs in
    * the given commits that are in the given references that belong to the given
    * repositories.
    *
    * {{{
    * val blobsDf = engine.getBlobs(repoIds, refNames, hashes)
    * }}}
    *
    * Calling this function with no arguments is the same as:
    *
    * {{{
    * engine.getRepositories.getReferences.getCommits.getTreeEntries.getBlobs
    * }}}
    *
    * @param repositoryIds  List of the repository ids to filter by (optional)
    * @param referenceNames List of reference names to filter by (optional)
    * @param commitHashes   List of commit hashes to filter by (optional)
    * @return [[org.apache.spark.sql.DataFrame]] with blobs of the given commits, refs and repos.
    */
  def getBlobs(repositoryIds: Seq[String] = Seq(),
               referenceNames: Seq[String] = Seq(),
               commitHashes: Seq[String] = Seq()): DataFrame = {
    val df = getRepositories

    var reposDf = df
    if (repositoryIds.nonEmpty) {
      reposDf = reposDf.filter(reposDf("id").isin(repositoryIds: _*))
    }

    var refsDf = reposDf.getReferences
    if (referenceNames.nonEmpty) {
      refsDf = refsDf.filter(refsDf("name").isin(referenceNames: _*))
    }

    var commitsDf = refsDf.getCommits
    if (commitHashes.nonEmpty) {
      commitsDf = commitsDf.getAllReferenceCommits.filter(commitsDf("hash").isin(commitHashes: _*))
    }

    commitsDf.getTreeEntries.getBlobs
  }

  /**
    * This method is only offered for easier usage from Python.
    */
  private[engine] def getBlobs(repositoryIds: java.util.List[String],
                               referenceNames: java.util.List[String],
                               commitHashes: java.util.List[String]): DataFrame =
    getBlobs(
      asScalaBuffer(repositoryIds),
      asScalaBuffer(referenceNames),
      asScalaBuffer(commitHashes)
    )

}

/**
  * Factory for [[tech.sourced.engine.Engine]] instances.
  */
object Engine {
  /**
    * Creates a new Engine instance with the given Spark session and
    * configures the database parameters.
    *
    * {{{
    * import tech.sourced.engine._
    *
    * val engine = Engine(sparkSession, "jdbc:mysql://host:post/db_name", "user", "password")
    * }}}
    *
    * @param session    spark session to use
    * @param dbUrl      URL for the database connection
    * @param dbUser     user for the database connection
    * @param dbPassword password for the database connection
    * @return Engine instance
    */
  def apply(session: SparkSession, dbUrl: String, dbUser: String, dbPassword: String): Engine = {
    new Engine(session, dbUrl, dbUser, dbPassword)
  }
}

