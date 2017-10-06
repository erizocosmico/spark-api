package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.api.util.CompiledExpression

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class RepositoryIterator(finalColumns: Array[String],
                         columns: Array[String],
                         repo: Repository,
                         filters: Seq[CompiledExpression])
  extends RootedRepoIterator[String](finalColumns, columns, repo, null, filters) {

  override protected def loadIterator(filters: Seq[CompiledExpression]): Iterator[String] =
    RepositoryIterator.loadIterator(repo, filters, "id")


  override protected def mapColumns(uuid: String): Map[String, () => Any] = {
    val c: StoredConfig = repo.getConfig
    val urls: Array[String] = c.getStringList("remote", uuid, "url")
    val isFork: Boolean = c.getBoolean("remote", uuid, "isfork", false)

    Map[String, () => Any](
      "id" -> (() => {
        RootedRepo.getRepositoryId(repo, uuid).get
      }),
      "urls" -> (() => urls),
      "is_fork" -> (() => isFork)
    )
  }
}

object RepositoryIterator {

  def loadIterator(repo: Repository, filters: Seq[CompiledExpression], repoKey: String): Iterator[String] = {
    val ids = filters.flatMap(_.matchingCases).flatMap {
      case (k, repoIds) if k == repoKey => repoIds.map(_.toString)
      case _ => Seq()
    }

    repo.getConfig.getSubsections("remote").asScala.toIterator.filter(ids.isEmpty || ids.contains(_))
  }

}
