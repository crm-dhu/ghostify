package com.goku.ghostify

import com.goku.ghostify.nlp.BertPipelineModel
import com.goku.ghostify.tensorflow.TensorflowWrapper
import com.goku.ghostify.util.LoadExternalModel.loadTextAsset
import com.goku.ghostify.util.{Params, ResourceHelper}

object CoreAnonymizer {

  val localModelPath = ResourceHelper.copyToLocal(Params.ModelPath)
  val vocabs: Map[String, Int] = loadTextAsset(localModelPath, "vocab.txt").zipWithIndex.toMap
  val id2labels: Map[Int, String] = loadTextAsset(localModelPath, "labels.txt").zipWithIndex.map {
    case (id, label) => (label, id)
  }.toMap

  val (wrapper, signatures) =
    TensorflowWrapper.read(localModelPath)

  val model = new BertPipelineModel(wrapper, vocabs, id2labels, signatures)

  def predict(texts: Seq[String]): Seq[String] = {
    val preds = model.predict(texts)
    preds.map(r => Params.EmailRegex.replaceAllIn(r, "[EMAIL]"))

  }

}
