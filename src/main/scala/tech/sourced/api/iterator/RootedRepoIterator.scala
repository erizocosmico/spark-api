package tech.sourced.api.iterator

import org.apache.spark.sql.Row
import org.eclipse.jgit.lib.{Repository, StoredConfig}
import tech.sourced.api.util.{CompiledExpression, GitUrlsParser}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

abstract class RootedRepoIterator[T](finalColumns: Array[String],
                                     columns: Array[String],
                                     repo: Repository,
                                     prevIter: RootedRepoIterator[_],
                                     filters: Seq[CompiledExpression]) extends Iterator[Row] {

  type RawRow = Map[String, () => Any]

  private var iter: Iterator[T] = _
  protected var currentRow: RawRow = _

  protected def loadIterator(filters: Seq[CompiledExpression]): Iterator[T]

  protected def getFilters: Seq[CompiledExpression] = filters

  protected def mapColumns(obj: T): RawRow

  override def hasNext: Boolean = {
    if (prevIter == null) {
      if (iter == null) {
        iter = loadIterator(getFilters)
      }

      iter.hasNext
    } else if (iter == null) {
      if (prevIter != null && !prevIter.hasNext) {
        return false
      }

      currentRow = prevIter.nextRaw
      iter = loadIterator(getFilters)
      iter.hasNext
    } else if (!iter.hasNext) {
      if (prevIter != null && !prevIter.hasNext) {
        return false
      }

      currentRow = prevIter.nextRaw
      iter = loadIterator(getFilters)
      iter.hasNext
    } else {
      true
    }
  }

  override def next: Row = {
    val mappedValues = if (currentRow != null) {
      currentRow ++ mapColumns(iter.next)
    } else {
      mapColumns(iter.next)
    }

    val values = finalColumns.map(c => mappedValues(c)())
    Row(values: _*)
  }


  def nextRaw: RawRow = {
    val row = mapColumns(iter.next())
    if (currentRow != null) {
      currentRow ++ row
    } else {
      row
    }
  }
}

object RootedRepo {

  private[iterator] def getRepositoryId(repo: Repository, uuid: String): Option[String] = {
    // TODO maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(i => i == uuid) match {
      case None => None
      case Some(i) => Some(GitUrlsParser.getIdFromUrls(c.getStringList("remote", i, "url")))
    }
  }

  private[iterator] def getRepositoryUUID(repo: Repository, id: String): Option[String] = {
    // TODO maybe a cache here could improve performance
    val c: StoredConfig = repo.getConfig
    c.getSubsections("remote").asScala.find(uuid => {
      val actualId: String =
        GitUrlsParser.getIdFromUrls(c.getStringList("remote", uuid, "url"))

      actualId == id
    })
  }

  private[iterator] def parseRef(repo: Repository, ref: String): (String, String) = {
    val split: Array[String] = ref.split("/")
    val uuid: String = split.last
    val repoId: String = getRepositoryId(repo, uuid).get
    val refName: String = split.init.mkString("/")

    (repoId, refName)
  }

}
