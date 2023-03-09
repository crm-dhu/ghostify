package com.salesforce.mce.ghostify

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

object Bert {

  val spark: SparkSession = SparkSession.builder
    .appName("spark-nlp-starter")
    .master("local[*]")
    .getOrCreate

  def main(args: Array[String]): Unit = {

    val document = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val token = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val modelPath = "/Users/donglin/Workspace/ghostify/utils/dslim/bert-base-NER/saved_model/1"

    val tokenClassifier = BertForTokenClassification.loadSavedModel(modelPath, spark)
      .setInputCols("document", "token")
      .setOutputCol("ner")
      .setCaseSensitive(true)
      .setMaxSentenceLength(128)

    val pipeline = new Pipeline().setStages(
      Array(
        document,
        token,
        tokenClassifier
      ))

    val testData = spark
      .createDataFrame(
        Seq(
          (1, "Google has announced the release of a beta version of the popular TensorFlow machine learning library"),
          (2, "The Paris metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards."),
          (3, "My name Alex Wang and my email address is alex.wang@gmail.com.")))
      .toDF("id", "text")

    val prediction = pipeline.fit(testData).transform(testData)
    prediction.select("text", "ner.result").show(false)

  }

}
