package com.salesforce.mce.ghostify.nlp

import org.apache.spark.ml.Model
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

trait HasSimpleAnnotate[M <: Model[M]] {

  this: AnnotatorModel[M] =>

  def annotate(annotations: Seq[Annotation]): Seq[Annotation]

  def dfAnnotate: UserDefinedFunction = udf { annotationProperties: Seq[AnnotationContent] =>
    annotate(annotationProperties.flatMap(_.map(Annotation(_))))
  }

}
