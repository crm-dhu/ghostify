package com.goku.ghostify.common

case class TokenPiece(
  wordpiece: String,
  token: String,
  pieceId: Int,
  isWordStart: Boolean,
  begin: Int,
  end: Int
)
