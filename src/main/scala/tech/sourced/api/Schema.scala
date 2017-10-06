package tech.sourced.api

import org.apache.spark.sql.types._

private[api] object Schema {
  val repositories = StructType(
    StructField("id", StringType, nullable = false) ::
      StructField("urls", ArrayType(StringType, containsNull = false), nullable = false) ::
      StructField("is_fork", BooleanType) ::
      Nil
  )

  val references = StructType(
    StructField("repository_id", StringType, nullable = false) ::
      StructField("name", StringType, nullable = false) ::
      StructField("hash", StringType, nullable = false) ::
      Nil
  )

  val commits = StructType(
    StructField("repository_id", StringType, nullable = false) ::
      StructField("reference_name", StringType, nullable = false) ::
      StructField("index", IntegerType, nullable = false) ::
      StructField("hash", StringType, nullable = false) ::
      StructField("message", StringType, nullable = false) ::
      StructField("parents", ArrayType(StringType, containsNull = false)) ::
      StructField("tree", MapType(StringType, StringType, valueContainsNull = false)) ::
      StructField("blobs", ArrayType(StringType, containsNull = false)) ::
      StructField("parents_count", IntegerType, nullable = false) ::

      StructField("author_email", StringType) ::
      StructField("author_name", StringType) ::
      StructField("author_date", TimestampType) ::

      StructField("committer_email", StringType) ::
      StructField("committer_name", StringType) ::
      StructField("committer_date", TimestampType) ::

      Nil
  )

  val files = StructType(
    StructField("file_hash", StringType, nullable = false) ::
      StructField("content", BinaryType) ::
      StructField("commit_hash", StringType, nullable = false) ::
      StructField("is_binary", BooleanType, nullable = false) ::
      StructField("path", StringType) ::

      Nil
  )

}
