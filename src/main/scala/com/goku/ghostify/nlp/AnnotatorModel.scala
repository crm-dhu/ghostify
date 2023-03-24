package com.goku.ghostify.nlp

import org.apache.spark.ml.{Model, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

abstract class AnnotatorModel[M <: Model[M]] extends RawAnnotator[M] with CanBeLazy {

  protected type AnnotationContent = Seq[Row]

  protected def beforeAnnotate(dataset: Dataset[_]): Dataset[_] = dataset

  protected def afterAnnotate(dataset: DataFrame): DataFrame = dataset

  protected def _transform(
    dataset: Dataset[_],
    recursivePipeline: Option[PipelineModel]
  ): DataFrame = {
    require(
      validate(dataset.schema),
      s"Wrong or missing inputCols annotators in $uid.\n" +
        msgHelper(dataset.schema) +
        s"\nMake sure such annotators exist in your pipeline, " +
        s"with the right output names and that they have following annotator types: " +
        s"${inputAnnotatorTypes.mkString(", ")}"
    )

    val inputDataset = beforeAnnotate(dataset)

    val processedDataset = {
      this match {
        case withAnnotate: HasSimpleAnnotate[M] =>
          inputDataset.withColumn(
            getOutputCol,
            wrapColumnMetadata({
              this match {
                case a: HasRecursiveTransform[M] =>
                  a.dfRecAnnotate(recursivePipeline.get)(
                    array(getInputCols.map(c => dataset.col(c)): _*)
                  )
                case _ =>
                  withAnnotate.dfAnnotate(array(getInputCols.map(c => dataset.col(c)): _*))
              }
            })
          )
        case _ =>
          throw new MatchError(s"${this.getClass.getName} doesn't have HasSimpleAnnotate interface")
      }
    }

    afterAnnotate(processedDataset)
  }

  override final def transform(dataset: Dataset[_]): DataFrame = {
    _transform(dataset, None)
  }

}
