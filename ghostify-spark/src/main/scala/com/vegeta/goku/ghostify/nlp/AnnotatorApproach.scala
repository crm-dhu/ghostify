package com.salesforce.mce.ghostify.nlp

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.ml.{Estimator, Model, PipelineModel}
import org.apache.spark.sql.types.{ArrayType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class AnnotatorApproach[M <: Model[M]]
    extends Estimator[M] with HasInputAnnotationCols with HasOutputAnnotationCol
    with HasOutputAnnotatorType with DefaultParamsWritable with CanBeLazy {

  val description: String

  def train(dataset: Dataset[_], recursivePipeline: Option[PipelineModel] = None): M

  def beforeTraining(spark: SparkSession): Unit = {}

  def onTrained(model: M, spark: SparkSession): Unit = {}

  protected def validate(schema: StructType): Boolean = {
    inputAnnotatorTypes.forall { inputAnnotatorType =>
      checkSchema(schema, inputAnnotatorType)
    }
  }

  protected def _fit(dataset: Dataset[_], recursiveStages: Option[PipelineModel]): M = {
    beforeTraining(dataset.sparkSession)
    val model = copyValues(train(dataset, recursiveStages).setParent(this))
    onTrained(model, dataset.sparkSession)
    model
  }

  override final def fit(dataset: Dataset[_]): M = {
    _fit(dataset, None)
  }

  override final def copy(extra: ParamMap): Estimator[M] = defaultCopy(extra)

  override final def transformSchema(schema: StructType): StructType = {
    require(
      validate(schema),
      s"Wrong or missing inputCols annotators in $uid.\n" +
        msgHelper(schema) +
        s"\nMake sure such annotators exist in your pipeline, " +
        s"with the right output names and that they have following annotator types: " +
        s"${inputAnnotatorTypes.mkString(", ")}"
    )
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
