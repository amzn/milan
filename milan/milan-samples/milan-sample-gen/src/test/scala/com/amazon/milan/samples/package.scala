package com.amazon.milan

import com.amazon.milan.compiler.flink.testing.TestApplicationExecutor


package object samples {
  def getTestApplicationExecutor: TestApplicationExecutor = {
    val executor = new TestApplicationExecutor()
    executor.addToClassPath(getClass)
    executor
  }
}
