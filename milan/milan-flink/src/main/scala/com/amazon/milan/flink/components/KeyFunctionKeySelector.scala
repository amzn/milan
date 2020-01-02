package com.amazon.milan.flink.components

import com.amazon.milan.flink.compiler.internal.RuntimeCompiledFunction
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.java.functions.KeySelector


object KeyFunctionKeySelector {
  val typeName: String = getClass.getTypeName.stripSuffix("$")
}


/**
 * A Flink [[KeySelector]] that uses a key extractor function supplied as a Milan [[FunctionDef]].
 *
 * @param inputType Type descriptor describing the input stream.
 * @param keyFunc   The key extractor function.
 * @tparam T    The input stream record type.
 * @tparam TKey The output key type.
 */
class KeyFunctionKeySelector[T, TKey](inputType: TypeDescriptor[_],
                                      keyFunc: FunctionDef) extends KeySelector[T, TKey] {
  private val compiledKeyFunction = new RuntimeCompiledFunction[T, TKey](this.inputType, this.keyFunc)

  override def getKey(in: T): TKey = {
    this.compiledKeyFunction(in)
  }
}
