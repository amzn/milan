package com.amazon.milan.flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.language.implicitConversions


package object testutil {
  def getTestExecutionEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setBufferTimeout(0)
    env
  }
}
