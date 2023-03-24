package com.goku.ghostify.nlp

import scala.util.Try

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

import AnnotatorType.DOCUMENT

class DocumentAssembler(override val uid: String)
    extends Transformer with DefaultParamsWritable with HasOutputAnnotatorType
    with HasOutputAnnotationCol {

  def this() = this(Identifiable.randomUID("document"))

  val inputCol: Param[String] =
    new Param[String](this, "inputCol", "input text column for processing")

  setDefault(outputCol -> DOCUMENT)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def getInputCol: String = $(inputCol)

  final val EmptyStr = ""

  def assemble(text: String, metadata: Map[String, String]): Seq[Annotation] = {
    val _text = Option(text).getOrElse(EmptyStr)
    Try(Seq(Annotation(outputAnnotatorType, 0, _text.length - 1, _text, metadata))).toOption
      .getOrElse(Seq.empty[Annotation])
  }

  def assembleFromArray(texts: Seq[String]): Seq[Annotation] = {
    texts.zipWithIndex.flatMap { case (text, idx) =>
      assemble(text, Map("sentence" -> idx.toString))
    }
  }

  private def dfAssembleNoExtras: UserDefinedFunction = udf { text: String =>
    assemble(text, Map("sentence" -> "0"))
  }

  private def dfAssemblyFromArray: UserDefinedFunction = udf { texts: Seq[String] =>
    assembleFromArray(texts)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val metadataBuilder: MetadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", outputAnnotatorType)
    val documentAnnotations =
      if (
        dataset.schema.fields
          .find(_.name == getInputCol)
          .getOrElse(
            throw new IllegalArgumentException(s"Dataset does not have any '$getInputCol' column")
          )
          .dataType == ArrayType(StringType, containsNull = false)
      )
        dfAssemblyFromArray(dataset.col(getInputCol))
      else
        dfAssembleNoExtras(dataset.col(getInputCol))
    dataset.withColumn(getOutputCol, documentAnnotations.as(getOutputCol, metadataBuilder.build))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override val outputAnnotatorType: AnnotatorType = DOCUMENT
  override def transformSchema(schema: StructType): StructType = {
    val metadataBuilder: MetadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", outputAnnotatorType)
    val outputFields = schema.fields :+
      StructField(
        getOutputCol,
        ArrayType(Annotation.dataType),
        nullable = false,
        metadataBuilder.build
      )
    StructType(outputFields)
  }
}

object DocumentAssembler extends DefaultParamsReadable[DocumentAssembler]
