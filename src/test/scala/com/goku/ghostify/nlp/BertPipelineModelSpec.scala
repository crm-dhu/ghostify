package com.goku.ghostify.nlp

import com.goku.ghostify.tensorflow.TensorflowWrapper
import com.goku.ghostify.util.LoadExternalModel.loadTextAsset
import com.goku.ghostify.util.{Params, ResourceHelper}
import org.scalatest.wordspec.AnyWordSpec

class BertPipelineModelSpec extends AnyWordSpec {

  val localModelPath = ResourceHelper.copyToLocal(Params.ModelPath)
  val vocabs: Map[String, Int] = loadTextAsset(localModelPath, "vocab.txt").zipWithIndex.toMap
  val id2labels: Map[Int, String] = loadTextAsset(localModelPath, "labels.txt").zipWithIndex.map {
    case (id, label) => (label, id)
  }.toMap

  val (wrapper, signatures) =
    TensorflowWrapper.read(localModelPath)

  val model = new BertPipelineModel(wrapper, vocabs, id2labels, signatures)

  "Bert model" should {
    "predict labels correctly" in {
      val data = Seq(
        "Google has announced the release of a beta version of the popular TensorFlow machine learning library",
        "The Paris metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.",
        "Happy birthday, Alex!"
      )

      val expected = Seq(
        "[ORG] has announced the release of a beta version of the popular [MISC] machine learning library",
        "The [LOC] metro will soon enter the 21st century, ditching single-use paper tickets for rechargeable electronic cards.",
        "Happy birthday, [PER]!"
      )
      val preds = model.predict(data)
      expected.zip(preds).foreach { case (e, p) => assert(e === p) }
    }
  }

}
