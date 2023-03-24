package com.goku.ghostify.nlp

import org.apache.spark.ml.param.{Params, StringArrayParam}
import org.apache.spark.sql.types.StructType

trait HasInputAnnotationCols extends Params {

  val inputAnnotatorTypes: Array[String]

  val optionalInputAnnotatorTypes: Array[String] = Array()

  protected final val inputCols: StringArrayParam =
    new StringArrayParam(this, "inputCols", "the input annotation columns")

  def setInputCols(value: Array[String]): this.type = {
    if (optionalInputAnnotatorTypes.isEmpty) {
      require(
        value.length == inputAnnotatorTypes.length,
        s"setInputCols in ${this.uid} expecting ${inputAnnotatorTypes.length} columns. " +
          s"Provided column amount: ${value.length}. " +
          s"Which should be columns from the following annotators: ${inputAnnotatorTypes.mkString(", ")} "
      )
    } else {
      val expectedColumns = inputAnnotatorTypes.length + optionalInputAnnotatorTypes.length
      require(
        value.length == inputAnnotatorTypes.length || value.length == expectedColumns,
        s"setInputCols in ${this.uid} expecting at least ${inputAnnotatorTypes.length} columns. " +
          s"Provided column amount: ${value.length}. " +
          s"Which should be columns from at least the following annotators: ${inputAnnotatorTypes
            .mkString(", ")} "
      )
    }
    set(inputCols, value)
  }

  protected def msgHelper(schema: StructType): String = {
    val schemaInfo = schema.map(sc =>
      (
        "column_name=" + sc.name,
        "is_nlp_annotator=" + sc.metadata.contains("annotatorType") + {
          if (sc.metadata.contains("annotatorType"))
            ",type=" + sc.metadata.getString("annotatorType")
          else ""
        }
      )
    )
    s"\nCurrent inputCols: ${getInputCols.mkString(",")}. Dataset's columns:\n${schemaInfo.mkString("\n")}."
  }

  final protected def checkSchema(schema: StructType, inputAnnotatorType: String): Boolean = {
    schema.exists { field =>
      {
        field.metadata.contains("annotatorType") &&
        field.metadata.getString("annotatorType") == inputAnnotatorType &&
        getInputCols.contains(field.name)
      }
    }
  }

  final def setInputCols(value: String*): this.type = setInputCols(value.toArray)

  def getInputCols: Array[String] =
    get(inputCols)
      .orElse(getDefault(inputCols))
      .getOrElse(
        throw new Exception(
          s"inputCols not provided." +
            s" Requires columns for ${inputAnnotatorTypes.mkString(", ")} annotators"
        )
      )

}
