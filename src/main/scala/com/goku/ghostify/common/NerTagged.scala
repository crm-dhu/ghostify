package com.goku.ghostify.common

case class NerTagged(
  wordpiece: String,
  token: String,
  tag: String,
  score: Double,
  isWordStart: Boolean,
  begin: Int,
  end: Int
)
