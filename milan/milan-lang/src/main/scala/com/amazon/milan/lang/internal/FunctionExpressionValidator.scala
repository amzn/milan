package com.amazon.milan.lang.internal

import com.amazon.milan.program.FunctionDef

import scala.reflect.macros.whitebox


trait FunctionExpressionValidator {
  def apply(c: whitebox.Context, function: FunctionDef): Unit
}
