package tech.sourced.api.iterator

import java.sql.Timestamp

import org.apache.spark.unsafe.types.UTF8String
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.errors.IncorrectObjectTypeException
import org.eclipse.jgit.lib.{ObjectId, Ref, Repository}
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.treewalk.TreeWalk
import tech.sourced.api.util.{Attr, CompiledExpression, EqualExpr}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CommitIterator(finalColumns: Array[String],
                     columns: Array[String],
                     repo: Repository,
                     prevIter: RootedRepoIterator[_],
                     filters: Seq[CompiledExpression])
  extends RootedRepoIterator[ReferenceWithCommit](finalColumns, columns, repo, prevIter, filters) {

  override def getFilters: Seq[CompiledExpression] = {
    if (currentRow != null) {
      val id = currentRow("repository_id")().asInstanceOf[String]
      val refName = currentRow("name")().asInstanceOf[String]
      filters ++ Seq(
        EqualExpr(Attr("repository_id", "commits"), id),
        EqualExpr(Attr("reference_name", "commits"), refName)
      )
    } else {
      filters
    }
  }

  override protected def loadIterator(filters: Seq[CompiledExpression]): Iterator[ReferenceWithCommit] =
    CommitIterator.loadIterator(repo, filters)

  override protected def mapColumns(obj: ReferenceWithCommit): Map[String, () => Any] = {
    val (repoId, refName) = RootedRepo.parseRef(repo, obj.ref.getName)

    val c: RevCommit = obj.commit
    lazy val files: Map[String, String] = this.getFiles(obj.commit)

    Map[String, () => Any](
      "repository_id" -> (() => repoId),
      "reference_name" -> (() => refName),
      "index" -> (() => obj.index),
      "hash" -> (() => ObjectId.toString(c.getId)),
      "message" -> (() => c.getFullMessage),
      "parents" -> (() => c.getParents.map(p => ObjectId.toString(p.getId))),
      "tree" -> (() => files),
      "blobs" -> (() => files.values.toArray),
      "parents_count" -> (() => c.getParentCount),

      "author_email" -> (() => c.getAuthorIdent.getEmailAddress),
      "author_name" -> (() => c.getAuthorIdent.getName),
      "author_date" -> (() => new Timestamp(c.getAuthorIdent.getWhen.getTime)),

      "committer_email" -> (() => c.getCommitterIdent.getEmailAddress),
      "committer_name" -> (() => c.getCommitterIdent.getName),
      "committer_date" -> (() => new Timestamp(c.getCommitterIdent.getWhen.getTime))
    )
  }

  private def getFiles(c: RevCommit): Map[String, String] = {
    val treeWalk: TreeWalk = new TreeWalk(repo)
    val nth: Int = treeWalk.addTree(c.getTree.getId)
    treeWalk.setRecursive(false)

    Stream.continually(treeWalk)
      .takeWhile(_.next()).map(tree => {
      if (tree.isSubtree) {
        tree.enterSubtree()
      }
      tree
    })
      .filter(!_.isSubtree)
      .map(
        walker => new String(walker.getRawPath) -> ObjectId.toString(walker.getObjectId(nth))).toMap
  }
}

case class ReferenceWithCommit(ref: Ref, commit: RevCommit, index: Int)

object CommitIterator {

  def loadIterator(repo: Repository, filters: Seq[CompiledExpression]): Iterator[ReferenceWithCommit] = {
    val refs = ReferenceIterator.loadIterator(repo, filters, "repository_id", "reference_name")
    new RefWithCommitIterator(repo, refs)
  }

}

class RefWithCommitIterator(repo: Repository, refs: Iterator[Ref]) extends Iterator[ReferenceWithCommit] {
  var actualRef: Ref = _
  var commits: Iterator[RevCommit] = _
  var index: Int = 0

  override def hasNext: Boolean = {
    while ((commits == null || !commits.hasNext) && refs.hasNext) {
      actualRef = refs.next()
      index = 0
      commits =
        try {
          Git.wrap(repo).log()
            .add(Option(actualRef.getPeeledObjectId).getOrElse(actualRef.getObjectId))
            .call().asScala.toIterator
        } catch {
          case _: IncorrectObjectTypeException => null
          // TODO log this
          // This reference is pointing to a non commit object
        }
    }

    refs.hasNext || (commits != null && commits.hasNext)
  }

  override def next(): ReferenceWithCommit = {
    val result: ReferenceWithCommit = ReferenceWithCommit(actualRef, commits.next(), index)
    index += 1
    result
  }
}