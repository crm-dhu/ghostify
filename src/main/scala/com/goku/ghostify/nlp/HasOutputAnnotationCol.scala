package com.goku.ghostify.nlp

import org.apache.spark.ml.param.{Param, Params}

trait HasOutputAnnotationCol extends Params {

  protected final val outputCol: Param[String] =
    new Param(this, "outputCol", "the output annotation column")

  final def setOutputCol(value: String): this.type = set(outputCol, value)
  final def getOutputCol: String = $(outputCol)
}
