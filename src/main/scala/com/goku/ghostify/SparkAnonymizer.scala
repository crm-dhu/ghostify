package com.goku.ghostify

import com.goku.ghostify.nlp.BertPipelineSparkModel
import com.goku.ghostify.util.Params
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkAnonymizer {

  private final val InputCol = "text"
  private final val OutputCol = "predictions"

  def apply(input: RDD[String])(implicit ss: SparkSession): RDD[String] = {

    import ss.implicits._

    val bertModel = new BertPipelineSparkModel()
      .setInputCol(InputCol)
      .setOutputCol(OutputCol)
      .setSavedModelPath(Params.ModelPath)

    val transformed = bertModel.transform(input.toDF(InputCol))

    transformed
      .select(OutputCol)
      .rdd
      .map(r => Params.EmailRegex.replaceAllIn(r.getString(0), "[EMAIL]"))

  }

}
