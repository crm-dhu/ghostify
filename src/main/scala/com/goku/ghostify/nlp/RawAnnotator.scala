package com.goku.ghostify.nlp

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._

trait RawAnnotator[M <: Model[M]]
    extends Model[M] with HasOutputAnnotatorType with HasInputAnnotationCols
    with HasOutputAnnotationCol {

  private def outputDataType: DataType = ArrayType(Annotation.dataType)

  protected def wrapColumnMetadata(col: Column): Column = {
    val metadataBuilder: MetadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", outputAnnotatorType)
    col.as(getOutputCol, metadataBuilder.build)
  }

  protected def validate(schema: StructType): Boolean = {
    inputAnnotatorTypes.forall { inputAnnotatorType =>
      checkSchema(schema, inputAnnotatorType)
    }
  }

  protected def extraValidateMsg = "Schema validation failed"

  protected def extraValidate(structType: StructType): Boolean = {
    true
  }

  override final def transformSchema(schema: StructType): StructType = {
    require(extraValidate(schema), extraValidateMsg)
    val metadataBuilder: MetadataBuilder = new MetadataBuilder()
    metadataBuilder.putString("annotatorType", outputAnnotatorType)
    val outputFields = schema.fields :+
      StructField(getOutputCol, outputDataType, nullable = false, metadataBuilder.build)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): M = defaultCopy(extra)
}
