package com.amazon.milan.flink.compiler

import com.amazon.milan.application.{ApplicationConfiguration, DataSink, DataSource}
import com.amazon.milan.flink.api.FlinkApplicationExtension
import com.amazon.milan.flink.application.{FlinkDataSink, FlinkDataSource}
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.flink.{MilanFlinkConfiguration, RuntimeEvaluator}
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.junit.Test

import scala.collection.JavaConverters._


object TestFlinkAppCustomSourceAndSink {
  var sourceValues: List[Any] = List.empty
  var sinkValues: List[Any] = List.empty


  @JsonSerialize
  class CustomSource[T: TypeDescriptor](values: List[T]) extends DataSource[T] {
    TestFlinkAppCustomSourceAndSink.sourceValues = values

    override def getTypeName: String = "TestFlinkAppCustomSourceAndSink.CustomSource"

    override def getGenericArguments: List[TypeDescriptor[_]] = List(implicitly[TypeDescriptor[T]])

    override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    }
  }


  @JsonDeserialize
  class FlinkCustomSource[T] extends FlinkDataSource[T] {
    private var recordTypeInformation: TypeInformation[T] = _

    override def addDataSource(env: StreamExecutionEnvironment): DataStreamSource[T] = {
      val values = TestFlinkAppCustomSourceAndSink.sourceValues.map(_.asInstanceOf[T])
      env.fromCollection(values.asJavaCollection, this.recordTypeInformation)
    }

    override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
      this.recordTypeInformation = RuntimeEvaluator.instance.createTypeInformation[T](genericArgs.head.asInstanceOf[TypeDescriptor[T]])
    }
  }


  @JsonSerialize
  class CustomSink[T: TypeDescriptor] extends DataSink[T] {
    override def getTypeName: String = "TestFlinkAppCustomSourceAndSink.CustomSink"

    override def getGenericArguments: List[TypeDescriptor[_]] = List(implicitly[TypeDescriptor[T]])

    override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    }
  }


  @JsonDeserialize
  class FlinkCustomSink[T] extends FlinkDataSink[T] {
    override def getSinkFunction: SinkFunction[_] = {
      new FlinkCustomSinkFunction[T]
    }

    override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    }
  }


  class FlinkCustomSinkFunction[T] extends SinkFunction[T] {
    override def invoke(value: T): Unit = {
      TestFlinkAppCustomSourceAndSink.synchronized {
        TestFlinkAppCustomSourceAndSink.sinkValues = TestFlinkAppCustomSourceAndSink.sinkValues :+ value
      }
    }
  }


  class Extension extends FlinkApplicationExtension {
    override def extensionId: String = "998095c1-e089-41d0-93e0-58a91dd8e128"

    override def getFlinkDataSinkClass(typeName: String): Option[Class[_ <: FlinkDataSink[_]]] = {
      if (typeName == "TestFlinkAppCustomSourceAndSink.CustomSink") {
        Some(classOf[FlinkCustomSink[_]])
      }
      else {
        None
      }
    }

    override def getFlinkDataSourceClass[T](typeName: String): Option[Class[_ <: FlinkDataSource[_]]] = {
      if (typeName == "TestFlinkAppCustomSourceAndSink.CustomSource") {
        Some(classOf[FlinkCustomSource[_]])
      }
      else {
        None
      }
    }
  }

}

import com.amazon.milan.flink.compiler.TestFlinkAppCustomSourceAndSink._


@Test
class TestFlinkAppCustomSourceAndSink {
  @Test
  def test_FlinkAppCustomSourceAndSink_CompilesAndRuns(): Unit = {
    val stream = Stream.of[IntRecord]

    val graph = new StreamGraph(stream)

    val config = new ApplicationConfiguration()
    config.setSource(stream, new CustomSource[IntRecord](List(IntRecord(1))))
    config.addSink(stream, new CustomSink[IntRecord])

    MilanFlinkConfiguration.instance.registerExtension(new Extension)

    val env = getTestExecutionEnvironment

    FlinkCompiler.defaultCompiler.compile(graph, config, env)

    env.executeThenWaitFor(() => TestFlinkAppCustomSourceAndSink.sinkValues.length == 1, 2)
  }
}
