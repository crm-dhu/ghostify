package com.goku.ghostify.data

import scala.util.Try

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

trait ObjectMarshaller[T] {

  def unmarshal(jsonObj: Json): Either[Throwable, T]

}

object ObjectMarshaller {

  case class Packet[T](className: String, content: T)

  def marshal[T: Encoder](obj: T): Json =
    Packet(obj.getClass().getCanonicalName(), obj).asJson

  def unmarshal[T](jsonObj: Json): Either[Throwable, T] = {
    val cursor = jsonObj.hcursor
    for {
      className <- cursor.downField("className").as[String]
      content = cursor.downField("content").focus.get
      result <- Try(
        Class
          .forName(className)
          .getMethod("unmarshal", classOf[Json])
          .invoke(null, content)
          .asInstanceOf[Either[Throwable, T]]
          .toOption
          .get
      ).toEither
    } yield result
  }

}
