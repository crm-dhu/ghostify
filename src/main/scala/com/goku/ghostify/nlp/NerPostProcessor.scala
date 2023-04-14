package com.goku.ghostify.nlp

import com.goku.ghostify.common.{NerMerged, NerTagged}

object NerPostProcessor {

  val NoChunk = "O"
  val BeginTagChunk = "B-"
  val ContinueTagChunk = "I-"

  def decode(tags: Seq[NerTagged]): Seq[NerMerged] = {
    val (groups, res) = tags.foldLeft((Seq.empty[Seq[NerTagged]], Seq.empty[NerTagged])) {
      case ((groups, curG), curT) =>
        if (curG.isEmpty) {
          if (isBeginChunk(curT)) (groups, Seq(updateChunk(curT)))
          else (groups, curG)
        } else {
          if (curT.tag == NoChunk) (groups :+ curG, Seq.empty[NerTagged])
          else {
            val lastTag = curG.last.tag
            val updatedNer = updateChunk(curT)
            if (!isBeginChunk(curT) && updatedNer.tag == lastTag)
              (groups, curG :+ updateChunk(curT))
            else (groups :+ curG, Seq(updatedNer))
          }
        }
    }
    val grouped = if (res.isEmpty) groups else groups :+ res
    grouped.map { rs =>
      val tag = rs.map(_.tag).head
      val scores = rs.map(_.score)
      val avgScore = scores.sum / scores.length
      val begin = rs.map(_.begin).min
      val end = rs.map(_.end).max
      NerMerged(tag, avgScore, begin, end)
    }
  }

  def isBeginChunk(tagged: NerTagged): Boolean = tagged.tag.startsWith(BeginTagChunk)

  def updateChunk(tagged: NerTagged): NerTagged =
    tagged.copy(tag = tagged.tag.stripPrefix(BeginTagChunk).stripPrefix(ContinueTagChunk))

}
