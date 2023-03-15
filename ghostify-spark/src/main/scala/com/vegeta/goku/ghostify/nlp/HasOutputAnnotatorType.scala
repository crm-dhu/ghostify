package com.vegeta.goku.ghostify.nlp

trait HasOutputAnnotatorType {
  type AnnotatorType = String
  val outputAnnotatorType: AnnotatorType
}
