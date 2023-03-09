package com.salesforce.mce.ghostify

import scala.reflect.runtime.universe._

import org.scalatest.wordspec.AnyWordSpec


class ClassUtilsSpec extends AnyWordSpec {

  case class Apple(id: Long, age: String, size: Int, color: String)

  "class accessors" should {

    "return list of class field with types in order" in {

      val classFieldTypes = ClassUtils.classAccessors[Apple]

      assertResult(4)(classFieldTypes.size)
      assert(classFieldTypes.map(_._1) === List("id", "age", "size", "color"))

      val result = List(
        "id" -> typeOf[Long],
        "age" -> typeOf[String],
        "size" -> typeOf[Int],
        "color" -> typeOf[String]
      )

      classFieldTypes.zip(result).foreach { case ((fieldName, fieldType), (theName, theType)) =>
        assert(fieldName === theName)
        assert(fieldType =:= theType)
      }
    }
  }
}

