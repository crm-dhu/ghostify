package com.goku.ghostify.data

import scala.collection.mutable.{Map => MutableMap}

/**
 * Note: this is currently not type safe, should try to see if we could refactor this with
 * shapeless's Extensible Record
 */
class FeatureMap private (private val untypedMap: MutableMap[NamedFeature[Any], Any]) {

  def put[T](feature: NamedFeature[T], value: T): this.type = {
    untypedMap += (feature -> value)
    this
  }

  def get[T](feature: NamedFeature[T]): Option[T] = {
    untypedMap.get(feature).asInstanceOf[Option[T]]
  }

  override def toString(): String = s"FeaturemMap(${untypedMap.toString()})"

}

object FeatureMap {

  type Kvp[T] = (NamedFeature[T], T)

  def apply(): FeatureMap = new FeatureMap(MutableMap.empty)

  def apply(kvps: Kvp[_]*): FeatureMap = new FeatureMap(MutableMap(kvps: _*))

}
