package com.salesforce.mce.ghostify

import com.johnsnowlabs.nlp.annotator.{BertForTokenClassification, NerConverter, SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Anonymizer {

  private final val InputCol = "text"
  private final val OutputCol = "predictions"

  def apply(input: RDD[String])(implicit ss: SparkSession): RDD[String] = {

    import ss.implicits._

    val document = new DocumentAssembler()
      .setInputCol(InputCol)
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols(document.getOutputCol)
      .setOutputCol("sentence")

    val token = new Tokenizer()
      .setInputCols(document.getOutputCol)
      .setOutputCol("token")

    val tokenClassifier = BertForTokenClassification
      .loadSavedModel(Params.ModelPath, ss)
      .setInputCols(document.getOutputCol, token.getOutputCol)
      .setOutputCol("ner")
      .setCaseSensitive(true)
      .setMaxSentenceLength(128)

    val nerConverter = new NerConverter()
      .setInputCols(sentenceDetector.getOutputCol, token.getOutputCol, tokenClassifier.getOutputCol)
      .setOutputCol(OutputCol)

    val pipeline = new Pipeline().setStages(
      Array(
        document,
        sentenceDetector,
        token,
        tokenClassifier,
        nerConverter
      )
    )

    val data = input.toDF(InputCol)

    val prediction = pipeline.fit(data).transform(data)

    val out = prediction.select(InputCol, OutputCol).as[NerResults]
    out.map { r =>
      r.predictions.filter(_.result.isDefined)
      val pos = r.predictions
        .filter(p => p.metadata.isDefined && p.metadata.get.contains("entity"))
        .map(p => (p.begin, p.end, p.metadata.get("entity")))
      val begin = 0 +: pos.map(_._2 + 1)
      val end = pos.map(_._1) :+ r.text.length
      val tag = pos.map(_._3) :+ ""

      val neTagged = begin
        .zip(end)
        .zip(tag)
        .map { case ((b, e), t) =>
          if (t.isEmpty) r.text.substring(b, e) else s"${r.text.substring(b, e)}[$t]"
        }
        .mkString("")
      Params.EmailRegex.replaceAllIn(neTagged, "[EMAIL]")
    }.rdd

  }

}
