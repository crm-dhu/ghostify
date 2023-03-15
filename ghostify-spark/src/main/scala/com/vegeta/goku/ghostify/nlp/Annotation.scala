package com.vegeta.goku.ghostify.nlp

import scala.collection.Map

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class Annotation(
  annotatorType: String,
  begin: Int,
  end: Int,
  result: String,
  metadata: Map[String, String],
  embeddings: Array[Float] = Array.emptyFloatArray
)

object Annotation {
  val dataType = new StructType(
    Array(
      StructField("annotatorType", StringType, nullable = true),
      StructField("begin", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false),
      StructField("result", StringType, nullable = true),
      StructField("metadata", MapType(StringType, StringType), nullable = true),
      StructField("embeddings", ArrayType(FloatType, false), true)
    )
  )

  val arrayType = new ArrayType(dataType, true)
  def apply(row: Row): Annotation = {
    Annotation(
      row.getString(0),
      row.getInt(1),
      row.getInt(2),
      row.getString(3),
      row.getMap[String, String](4),
      row.getSeq[Float](5).toArray
    )
  }
}
