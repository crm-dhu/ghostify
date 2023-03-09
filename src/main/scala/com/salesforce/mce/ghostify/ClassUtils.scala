package com.salesforce.mce.ghostify

import scala.reflect.runtime.universe._


object ClassUtils {

  /**
    * Get field names with corresponding type from case class
    */
  def classAccessors[T: TypeTag]: List[(String, Type)] = typeOf[T].decls.sorted.collect {
    case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType)
  }
}
