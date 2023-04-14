package com.goku.ghostify.tensorflow

import java.util

import scala.util.matching.Regex

import org.slf4j.{Logger, LoggerFactory}
import org.tensorflow.SavedModelBundle
import org.tensorflow.proto.framework.TensorInfo
import org.tensorflow.proto.util.SaverDef

object ModelSignatureManager {

  val KnownProviders: Array[String] = Array("TF1", "TF2")

  private[ModelSignatureManager] val logger: Logger =
    LoggerFactory.getLogger("ModelSignatureManager")

  def apply(
    tfSignatureType: String = "TF1",
    tokenIdsValue: String = ModelSignatureConstants.InputIdsV1.value,
    maskIdsValue: String = ModelSignatureConstants.AttentionMaskV1.value,
    segmentIdsValue: String = ModelSignatureConstants.TokenTypeIdsV1.value,
    embeddingsValue: String = ModelSignatureConstants.LastHiddenStateV1.value,
    sentenceEmbeddingsValue: String = ModelSignatureConstants.PoolerOutputV1.value
  ): Map[String, String] =
    tfSignatureType.toUpperCase match {
      case "TF1" =>
        Map[String, String](
          ModelSignatureConstants.InputIds.key -> tokenIdsValue,
          ModelSignatureConstants.AttentionMask.key -> maskIdsValue,
          ModelSignatureConstants.TokenTypeIds.key -> segmentIdsValue,
          ModelSignatureConstants.LastHiddenState.key -> embeddingsValue,
          ModelSignatureConstants.PoolerOutput.key -> sentenceEmbeddingsValue
        )
      case _ => throw new Exception("Model provider not available.")
    }

  def convertToAdoptedKeys(matched: Map[String, String]): Map[String, String] = {
    val SecondaryIndexSep = "::"
    matched
      .map { case (k, v) => k.split(SecondaryIndexSep)(1) -> v } // signature def name
      .map { case (k, v) => ModelSignatureConstants.toAdoptedKeys(k) -> v }
  }

  def getSignaturesFromModel(model: SavedModelBundle): Map[String, String] = {
    import collection.JavaConverters._

    val InputPrefix = "input"
    val OutputPrefix = "output"
    val Sep = "::"

    val modelSignatures = scala.collection.mutable.Map.empty[String, String]

    /**
     * Loop imperatively over signature definition to extract them in a map
     *
     * @param prefix
     *   : input or output attribute
     * @param signDefinitionsMap
     *   : Java signature definition map
     */
    def extractSignatureDefinitions(
      prefix: String,
      signDefinitionsMap: util.Map[String, TensorInfo]
    ): Unit = {
      for (e <- signDefinitionsMap.entrySet.asScala) {

        val key: String = e.getKey
        val tfInfo: TensorInfo = e.getValue

        modelSignatures +=
          (s"$prefix$Sep$key$Sep${ModelSignatureConstants.Name.key}" ->
            tfInfo.getName)
        modelSignatures +=
          (s"$prefix$Sep$key$Sep${ModelSignatureConstants.DType.key}" ->
            tfInfo.getDtype.toString)
        modelSignatures +=
          (s"$prefix$Sep$key$Sep${ModelSignatureConstants.DimCount.key}" ->
            tfInfo.getTensorShape.getDimCount.toString)
        modelSignatures +=
          (s"$prefix$Sep$key$Sep${ModelSignatureConstants.ShapeDimList.key}" ->
            tfInfo.getTensorShape.getDimList.toString
              .replaceAll("\n", "")
              .replaceAll("size:", ""))
        modelSignatures +=
          (s"$prefix$Sep$key$Sep${ModelSignatureConstants.SerializedSize.key}" ->
            tfInfo.getName)
      }
    }

    if (model.metaGraphDef.hasGraphDef && model.metaGraphDef.getSignatureDefCount > 0) {
      for (sigDef <- model.metaGraphDef.getSignatureDefMap.values.asScala) {
        // extract input sign map
        extractSignatureDefinitions(InputPrefix, sigDef.getInputsMap)
        // extract output sign map
        extractSignatureDefinitions(OutputPrefix, sigDef.getOutputsMap)
      }
    }

    modelSignatures.toMap
  }

  /** Regex matcher */
  def findTFKeyMatch(candidate: String, pattern: Regex): Boolean = {
    val _value = candidate.split("::")(1) // i.e. input::input_ids::name
    val res = pattern findAllIn _value
    if (res.nonEmpty)
      true
    else
      false
  }

  def extractSignatures(
    model: SavedModelBundle,
    saverDef: SaverDef
  ): Option[Map[String, String]] = {

    val signatureCandidates = getSignaturesFromModel(model)
    val signDefNames: Map[String, String] =
      signatureCandidates.filterKeys(_.contains(ModelSignatureConstants.Name.key))

    //    val modelProvider = classifyProvider(signDefNames)

    val adoptedKeys = convertToAdoptedKeys(signDefNames) + (
      "filenameTensorName_" -> saverDef.getFilenameTensorName.replaceAll(":0", ""),
      "restoreOpName_" -> saverDef.getRestoreOpName.replaceAll(":0", ""),
      "saveTensorName_" -> saverDef.getSaveTensorName.replaceAll(":0", "")
    )

    Option(adoptedKeys)
  }
}
