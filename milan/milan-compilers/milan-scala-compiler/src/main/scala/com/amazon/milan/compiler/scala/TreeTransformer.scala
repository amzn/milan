package com.amazon.milan.compiler.scala

import com.amazon.milan.program.FunctionDef


trait TreeTransformer {
  /**
   * Transforms a [[FunctionDef]] into one that is functionally equivalent.
   */
  def transform(functionDef: FunctionDef): FunctionDef
}


class IdentityTreeTransformer extends TreeTransformer {
  override def transform(functionDef: FunctionDef): FunctionDef = functionDef
}
