package com.amazon.milan.program.internal

import com.amazon.milan.program

import scala.reflect.macros.whitebox


class TreeMacros(val c: whitebox.Context) extends ConvertExpressionHost {
  def fromExpression(expr: c.universe.Tree): c.Expr[program.Tree] = {
    this.getMilanExpression(expr)
  }

  def fromFunction(expr: c.universe.Tree): c.Expr[program.FunctionDef] = {
    this.getMilanFunction(expr)
  }
}
