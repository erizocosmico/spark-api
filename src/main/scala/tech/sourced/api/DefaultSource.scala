package tech.sourced.api

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import tech.sourced.api.iterator._
import tech.sourced.api.provider.{RepositoryProvider, SivaRDDProvider}
import tech.sourced.api.util.Expression

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName = "git"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse(DefaultSource.tableNameKey, throw new SparkException("parameter 'table' must be provided"))

    val schema: StructType = table match {
      case "repositories" => Schema.repositories
      case "references" => Schema.references
      case "commits" => Schema.commits
      case "files" => Schema.files
      case other => throw new SparkException(s"table '$other' is not supported")
    }

    GitRelation(sqlContext, schema, tableSource = Some(table))
  }
}

object DefaultSource {
  val tableNameKey = "table"
  val pathKey = "path"
}

case class GitRelation(sqlContext: SQLContext,
                       schema: StructType,
                       joinConditions: Option[Expression] = None,
                       tableSource: Option[String] = None)
  extends BaseRelation with CatalystScan {

  private val localPath: String = sqlContext.getConf(localPathKey, "/tmp")
  private val path: String = sqlContext.getConf(repositoriesPathKey)
  private val skipCleanup: Boolean = sqlContext.sparkContext.getConf.getBoolean(skipCleanupKey, false)

  // TODO implement this correctly to avoid a lot of required columns push up to spark
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    super.unhandledFilters(filters)
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    // TODO remove prints
    println("JOIN CONDITIONS: ", joinConditions)
    println("SCHEMA: ", schema)
    println("SCHEMA METADATA: ", schema.map(_.metadata))
    println("FILTERS:", filters)
    println("REQUIRED COLS:", requiredColumns)
    println("REQUIRED COLS METADATA:", requiredColumns.map(_.metadata))
    println("PATH AND LOCAL PATH:", path, localPath)

    // TODO process filters

    // Generate a mapping of columns by their source
    val columnsBySource: Map[String, Array[String]] = tableSource match {
      case Some(ts) => Map(ts -> requiredColumns.map(_.name).toArray)
      case None =>
        requiredColumns
          .map(a => a.metadata.getString("source") -> a.name)
          .groupBy(_._1)
          .map { case (k, v) => (k, v.map(_._2).toArray) }
    }

    val sc = sqlContext.sparkContext
    val sivaRDD = SivaRDDProvider(sc).get(path)

    // Get and sort the sources according to the iterator hierarchy
    var sources = Seq(tableSource.getOrElse(""))
    if (tableSource.isEmpty) {
      sources = schema
        .map(_.metadata.getString("source"))
        .distinct
        .sortWith(Sources.compare(_, _) < 0)
    }

    val filtersBySource = filters.map(Expression.compile)
      .flatMap(_.expressions)
      .map(e => (e.sources.distinct, e))
      .filter(_._1.length == 1)
      .groupBy(_._1)
      .map { case (k, v) => (k.head, v.map(_._2)) }

    val requiredColsB = sc.broadcast(requiredColumns.map(_.name).toArray)
    val columnsBySourceB = sc.broadcast(columnsBySource)
    val localPathB = sc.broadcast(localPath)
    val sourcesB = sc.broadcast(sources)
    val filtersB = sc.broadcast(filtersBySource)

    sivaRDD.flatMap(pds => {
      val provider = RepositoryProvider(localPathB.value, skipCleanup)
      val repo = provider.get(pds)

      // since the sources are ordered by their hierarchy, we can chain them like this
      // using the last used iterator as input for the current one
      var iter: Option[RootedRepoIterator[_]] = None
      sourcesB.value.foreach({
        case k@"repositories" =>
          iter = Some(new RepositoryIterator(
            requiredColsB.value,
            columnsBySourceB.value.getOrElse(k, Array[String]()),
            repo,
            filtersB.value.getOrElse(k, Seq())
          ))

        case k@"references" =>
          iter = Some(new ReferenceIterator(
            requiredColsB.value,
            columnsBySourceB.value.getOrElse(k, Array[String]()),
            repo,
            iter.orNull,
            filtersB.value.getOrElse(k, Seq())
          ))

        case k@"commits" =>
          iter = Some(new CommitIterator(
            requiredColsB.value,
            columnsBySourceB.value.getOrElse(k, Array[String]()),
            repo,
            iter.orNull,
            filtersB.value.getOrElse(k, Seq())
          ))

        case k@"files" =>
          iter = Some(new CommitIterator(
            requiredColsB.value,
            columnsBySourceB.value.getOrElse(k, Array[String]()),
            repo,
            iter.orNull,
            filtersB.value.getOrElse(k, Seq())
          ))

        case other => throw new SparkException(s"required cols for '$other' is not supported")
      })

      new CleanupIterator(iter.getOrElse(Seq().toIterator), provider.close(pds.getPath()))
    })
  }
}

object Sources {

  val orderedSources = Array(
    "repositories",
    "references",
    "commits",
    "files"
  )

  def compare(a: String, b: String): Int = orderedSources.indexOf(a)
    .compareTo(orderedSources.indexOf(b))

}
