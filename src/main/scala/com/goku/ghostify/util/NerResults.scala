package com.goku.ghostify.util

case class NerResults(text: String, predictions: Seq[NerOutput])

case class NerOutput(
  annotatorType: Option[String],
  begin: Int,
  end: Int,
  result: Option[String],
  metadata: Option[Map[String, String]],
  embeddings: Option[Seq[Double]]
)
