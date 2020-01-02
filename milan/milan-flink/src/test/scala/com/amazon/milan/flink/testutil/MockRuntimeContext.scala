package com.amazon.milan.flink.testutil

import org.apache.flink.api.common.functions.RuntimeContext
import org.mockito.Mockito._


object MockRuntimeContext {
  def create(): RuntimeContext = {
    mock(classOf[RuntimeContext])
  }
}
