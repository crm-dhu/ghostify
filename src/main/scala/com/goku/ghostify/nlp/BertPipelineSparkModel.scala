package com.goku.ghostify.nlp

import scala.util.Try

import com.goku.ghostify.tensorflow.TensorflowWrapper
import com.goku.ghostify.util.LoadExternalModel.loadTextAsset
import com.goku.ghostify.util.ResourceHelper
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{BooleanParam, IntParam, Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

class BertPipelineSparkModel(override val uid: String)
    extends Transformer with HasInputCol with HasOutputCol with Logging {

  def this() = this(Identifiable.randomUID("BertPipelineSparkModel"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  val caseSensitive: BooleanParam = new BooleanParam(
    this,
    "caseSensitive",
    "whether to do a case-sensitive comparison over the word pairs"
  )

  def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)

  def getCaseSensitive: Boolean = $(caseSensitive)

  val batchSize: IntParam = new IntParam(this, "batchSize", "batch size for model prediction")

  def setBatchSize(value: Int): this.type = set(batchSize, value)

  def getBatchSize: Int = $(batchSize)

  val savedModelPath: Param[String] =
    new Param[String](this, "savedModelPath", "model path to the saved model")

  def setSavedModelPath(value: String): this.type = set(savedModelPath, value)

  def getSavedModelPath: String = $(savedModelPath)

  def maxSentenceLength: IntParam = new IntParam(
    this,
    "maxSentenceLength",
    "maximum length of input tokens for bert model (Maximum is 512"
  )

  def setMaxSentenceLength(value: Int): this.type = {
    require(
      value <= 512,
      "BERT models do not support sequences longer than 512 because of trainable positional embeddings."
    )
    require(value >= 1, "The maxSentenceLength must be at least 1")
    set(maxSentenceLength, value)
    this
  }

  def getMaxSentenceLength: Int = $(maxSentenceLength)

  setDefault(batchSize -> 1, maxSentenceLength -> 128, caseSensitive -> true)

  private var model: Option[Broadcast[BertPipelineModel]] = None

  def loadModel(): Option[BertPipelineModel] = {
    val localModelPath = ResourceHelper.copyToLocal($(savedModelPath))
    Try {
      val vocabs: Map[String, Int] = loadTextAsset(localModelPath, "vocab.txt").zipWithIndex.toMap
      val id2labels: Map[Int, String] =
        loadTextAsset(localModelPath, "labels.txt").zipWithIndex.map { case (id, label) =>
          (label, id)
        }.toMap
      val (wrapper, signatures) =
        TensorflowWrapper.read(localModelPath)

      new BertPipelineModel(
        wrapper,
        vocabs,
        id2labels,
        signatures,
        $(batchSize),
        $(maxSentenceLength),
        $(caseSensitive)
      )
    }.toOption
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  protected def validateInputType(inputType: DataType): Unit = require(inputType == StringType)

  override def transform(dataset: Dataset[_]): DataFrame = {
    import dataset.sparkSession.implicits._
    val sc = dataset.sparkSession.sparkContext
    if (model.isEmpty) model = loadModel().map(sc.broadcast(_))
    // todo: support batch size > 1
    dataset
      .select($(inputCol))
      .rdd
      .map(r => model.get.value.predict(Seq(r.getString(0))).head)
      .toDF($(outputCol))
  }

  protected def outputDataType: DataType = StringType

  override def copy(extra: ParamMap): BertPipelineSparkModel = defaultCopy(extra)
}
