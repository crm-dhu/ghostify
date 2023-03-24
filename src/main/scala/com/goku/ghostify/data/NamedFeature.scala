package com.goku.ghostify.data

import scala.reflect.ClassTag

/**
 * Implemented with case class because it is needed as look up keys
 */
case class NamedFeature[+T: ClassTag](name: String)
