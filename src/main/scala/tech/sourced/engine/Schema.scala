package tech.sourced.engine

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types._

/**
  * Schema contains all the schemas of the multiple tables offered by this library.
  */
private[engine] object Schema {

  /**
    * Repositories table schema.
    */
  val repositories = StructType(
    StructField("id", StringType, nullable = false) ::
      Nil
  )

  /**
    * Remotes table schema.
    */
  val remotes = StructType(
    StructField("repository_id", StringType, nullable = false) ::
    StructField("name", StringType, nullable = false) ::
    StructField("push_url", StringType, nullable = false) ::
    StructField("fetch_url", StringType, nullable = false) ::
    StructField("push_refspec", StringType, nullable = false) ::
    StructField("fetch_refspec", StringType, nullable = false) ::
    Nil
  )

  /**
    * References table schema.
    */
  val references = StructType(
    StructField("repository_id", StringType, nullable = false) ::
      StructField("name", StringType, nullable = false) ::
      StructField("hash", StringType, nullable = false) ::
      Nil
  )

  /**
    * Commits table schema containing all the data about commits.
    */
  val commits = StructType(
      StructField("hash", StringType, nullable = false) ::

      StructField("author_name", StringType, nullable = false) ::
      StructField("author_email", StringType, nullable = false) ::
      StructField("author_when", StringType, nullable = false) ::

        StructField("committer_name", StringType, nullable = false) ::
      StructField("committer_email", StringType, nullable = false) ::
      StructField("committer_when", StringType, nullable = false) ::

      StructField("message", StringType) ::
      StructField("tree_hash", StringType) ::
      Nil
  )

  /**
    * Tree Entries table schema containing all the tree entries data.
    */
  val treeEntries = StructType(
    StructField("tree_hash", StringType, nullable = false) ::
      StructField("entry_hash", StringType, nullable = false) ::
      StructField("mode", StringType, nullable = false) ::
      StructField("name", StringType, nullable = false) ::
      Nil
  )

  /**
    * Blobs table schema containing all the blobs data.
    */
  val blobs = StructType(
    StructField("hash", StringType, nullable = false) ::
      StructField("size", IntegerType, nullable = false) ::
      StructField("content", BinaryType) ::
      Nil
  )

  /**
    * Return the schema for the table with the given name. Throws a SparkException
    * if there is no schema for the given table.
    *
    * @param table name
    * @return schema for the table
    * @throws SparkException if the table does not exist
    */
  def apply(table: String): StructType = table match {
    case "repositories" => Schema.repositories
    case "remotes" => Schema.remotes
    case "refs" => Schema.references
    case "commits" => Schema.commits
    case "tree_entries" => Schema.treeEntries
    case "blobs" => Schema.blobs
    case other => throw new SparkException(s"table '$other' is not supported")
  }

}
