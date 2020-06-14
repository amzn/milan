package com.amazon.milan.compiler.flink

import com.amazon.milan.program.FunctionDef


package object internal {
  def validateFunctionArgsHaveTypes(functionDef: FunctionDef): Unit = {
    if (functionDef.arguments.exists(arg => arg.tpe == null)) {
      throw new IllegalArgumentException(s"One or more arguments are missing type information.")
    }
  }
}
