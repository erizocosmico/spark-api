package tech.sourced.api.iterator

import java.util

import org.apache.spark.internal.Logging
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.diff.RawText
import org.eclipse.jgit.lib.{ObjectId, ObjectReader, Repository}
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.treewalk.TreeWalk
import org.slf4j.Logger
import tech.sourced.api.util.CompiledExpression

import scala.collection.JavaConverters._


/**
  * Blob iterator: returns all blobs from the filtered commits
  *
  * @param requiredColumns
  * @param repo
  * @param filters
  */
class BlobIterator(finalColumns: Array[String],
                   columns: Array[String],
                   repo: Repository,
                   prevIter: RootedRepoIterator[_],
                   filters: Seq[CompiledExpression])
  extends RootedRepoIterator[CommitTree](finalColumns, columns, repo, prevIter, filters) with Logging {

  override protected def loadIterator(filters: Seq[CompiledExpression]): Iterator[CommitTree] = {
    var refs = new Git(repo).branchList().call().asScala.filter(!_.isSymbolic)

    val filtered = filters.toIterator
      .flatMap(_.matchingCases)
      .flatMap {
        case ("reference_name", filteredRefs) =>
          refs = refs.filter { ref =>
            val (_, refName) = RootedRepo.parseRef(repo, ref.getName)
            filteredRefs.contains(refName)
          }
          log.debug(s"Iterating all ${refs.size} refs")
          refs.toIterator.flatMap { ref =>
            log.debug(s" $ref")
            JGitBlobIterator(getTreeWalk(ref.getObjectId), log)
          }
        case ("hash", filteredHashes) =>
          filteredHashes.toIterator.flatMap { hash =>
            val commitId = ObjectId.fromString(hash.asInstanceOf[String])
            if (repo.hasObject(commitId)) {
              JGitBlobIterator(getTreeWalk(commitId), this.log)
            } else {
              Seq()
            }
          }
        case anyOtherFilter =>
          log.debug(s"BlobIterator does not support filter $anyOtherFilter")
          Seq()
      }

    if (filtered.hasNext) {
      filtered
    } else {
      log.debug(s"Iterating all ${refs.size} refs")
      refs.toIterator.flatMap { ref =>
        log.debug(s" $ref")
        JGitBlobIterator(getTreeWalk(ref.getObjectId), log)
      }
    }
  }

  override protected def mapColumns(commitTree: CommitTree): Map[String, () => Any] = {
    log.debug(s"Reading blob:${commitTree.tree.getObjectId(0).name()} of tree:${commitTree.tree.getPathString} from commit:${commitTree.commit.name()}")
    val content = BlobIterator.readFile(commitTree.tree.getObjectId(0), commitTree.tree.getObjectReader)
    val isBinary = RawText.isBinary(content)
    Map[String, () => Any](
      "file_hash" -> (() => commitTree.tree.getObjectId(0).name),
      "content" -> (() => if (isBinary) Array.emptyByteArray else content),
      "commit_hash" -> (() => commitTree.commit.name),
      "is_binary" -> (() => isBinary),
      "path" -> (() => commitTree.tree.getPathString)
    )
  }

  private def getTreeWalk(commitId: ObjectId) = {
    val revCommit = repo.parseCommit(commitId)

    val treeWalk = new TreeWalk(repo)
    treeWalk.setRecursive(true)
    treeWalk.addTree(revCommit.getTree)
    CommitTree(commitId, treeWalk)
  }
}

case class CommitTree(commit: ObjectId, tree: TreeWalk)

object BlobIterator {
  val readMaxBytes = 20 * 1024 * 1024

  /**
    * Read max N bytes of the given blob
    *
    * @param objId
    * @param reader
    * @param max maximum number of bytes to read in memory
    * @return
    */
  def readFile(objId: ObjectId, reader: ObjectReader, max: Integer = readMaxBytes): Array[Byte] = {
    val obj = reader.open(objId)
    val data = if (obj.isLarge) {
      val buf = Array.ofDim[Byte](max)
      val is = obj.openStream()
      is.read(buf)
      is.close()
      buf
    } else {
      obj.getBytes
    }
    reader.close()
    data
  }
}

/**
  * Iterates a Tree from a given commit, skipping missing blobs.
  * Must not produce un-reachable objects, as client has no way of dealing with it.
  *
  * @see [[BlobIterator#mapColumns]]
  */
class JGitBlobIterator[T <: CommitTree](commitTree: T, log: Logger) extends Iterator[T] {
  var wasAlreadyMoved = false

  override def hasNext: Boolean = {
    if (wasAlreadyMoved) {
      return true
    }
    val hasNext = try {
      moveIteratorSkippingMissingObj
    } catch {
      case e: Exception => log.error(s"Failed to iterate tree - due to ${e.getClass.getSimpleName}", e)
        false
    }
    wasAlreadyMoved = true
    if (!hasNext) {
      commitTree.tree.close()
    }
    hasNext
  }

  override def next(): T = {
    if (!wasAlreadyMoved) {
      moveIteratorSkippingMissingObj
    }
    wasAlreadyMoved = false
    commitTree
  }

  private def moveIteratorSkippingMissingObj: Boolean = {
    val hasNext = commitTree.tree.next()
    if (!hasNext) {
      return false
    }

    if (commitTree.tree.getObjectReader.has(commitTree.tree.getObjectId(0))) {
      true
    } else { // tree hasNext, but blob obj is missing
      log.debug(s"Skip non-existing ${commitTree.tree.getObjectId(0).name()} ")
      moveIteratorSkippingMissingObj
    }
  }

}

object JGitBlobIterator {
  def apply(commitTree: CommitTree, log: Logger) = new JGitBlobIterator(commitTree, log)
}


