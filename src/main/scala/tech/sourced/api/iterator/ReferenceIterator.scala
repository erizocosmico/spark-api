package tech.sourced.api.iterator

import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import tech.sourced.api.util.{Attr, CompiledExpression, EqualExpr}

import scala.collection.JavaConverters._

class ReferenceIterator(finalColumns: Array[String],
                        columns: Array[String],
                        repo: Repository,
                        prevIter: RootedRepoIterator[_],
                        filters: Seq[CompiledExpression])
  extends RootedRepoIterator[Ref](finalColumns, columns, repo, prevIter, filters) {

  override def getFilters: Seq[CompiledExpression] = {
    if (currentRow != null) {
      val id = currentRow("id")().asInstanceOf[String]
       filters ++ Seq(EqualExpr(Attr("repository_id", "references"), id))
    } else {
      filters
    }
  }

  protected def loadIterator(filters: Seq[CompiledExpression]): Iterator[Ref] =
    ReferenceIterator.loadIterator(repo, filters, "repository_id", "name")

  override protected def mapColumns(ref: Ref): Map[String, () => Any] = {
    val (repoId, refName) = RootedRepo.parseRef(repo, ref.getName)
    Map[String, () => Any](
      "repository_id" -> (() => {
        repoId
      }),
      "name" -> (() => {
        refName
      }),
      "hash" -> (() => {
        ObjectId.toString(ref.getObjectId)
      })
    )
  }

}

object ReferenceIterator {
  def loadIterator(repo: Repository,
                   filters: Seq[CompiledExpression],
                   repoKey: String,
                   refNameKey: String): Iterator[Ref] = {
    val referenceNames = filters.flatMap(_.matchingCases).flatMap {
      case (k, refNames) if k == refNameKey => refNames.map(_.toString)
      case _ => Seq()
    }

    val repoIds = RepositoryIterator.loadIterator(repo, filters, "repository_id").toArray

    repo.getAllRefs.asScala.values.toIterator.filter(ref => {
      val (repoId, refName) = RootedRepo.parseRef(repo, ref.getName)
      (repoIds.isEmpty || repoIds.contains(repoId)) &&
        (referenceNames.isEmpty || referenceNames.contains(refName))
    })
  }
}
