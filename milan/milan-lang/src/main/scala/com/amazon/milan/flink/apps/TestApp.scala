package com.amazon.milan.flink.apps

import com.amazon.milan.Id._
import com.amazon.milan.flink.application.FlinkApplicationConfiguration
import com.amazon.milan.flink.application.sinks.FlinkSingletonMemorySink
import com.amazon.milan.flink.application.sources.FlinkListDataSource
import com.amazon.milan.flink.compiler.FlinkCompiler
import com.amazon.milan.lang.{ObjectStream, StreamGraph}
import com.amazon.milan.program.ExternalStream
import com.amazon.milan.types._
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


object TestApp {

  def main(args: Array[String]): Unit = {
    // We have to create the graph node ourselves.
    // We can't call Stream.of[] because it uses a macro and this code is being compiled as part of the same module.
    val typeDesc = types.stream(
      TypeDescriptor.create[TestAppRecord](
        "com.amazon.milan.flink.apps.TestAppRecord",
        List(
          (RecordIdFieldName, types.String),
          ("value", types.Int))))

    val streamNode = ExternalStream(
      "id",
      "stream",
      typeDesc)

    val inputStream = new ObjectStream[TestAppRecord](streamNode)

    val sourceValues = List(1, 2, 3, 4, 5).map(new TestAppRecord(_))
    val source = new FlinkListDataSource[TestAppRecord](sourceValues, createTypeInformation[TestAppRecord])

    val graph = new StreamGraph(inputStream)

    val config = new FlinkApplicationConfiguration()
    config.setSource(inputStream, source)

    val sink = new FlinkSingletonMemorySink[TestAppRecord](typeDesc.recordType.asInstanceOf[TypeDescriptor[TestAppRecord]])

    config.addSink(inputStream, sink)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    env.execute()
  }
}


class TestAppRecord(val recordId: String, val value: Int) extends Record {
  def this() {
    this(newId(), 0)
  }

  def this(value: Int) {
    this(newId(), value)
  }

  override def getRecordId: String = this.recordId
}
