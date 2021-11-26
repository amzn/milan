package com.amazon.milan.samples.exec

import com.amazon.milan.compiler.flink.runtime.implicits._

class TimeAssigner_e98a8fa3_8f7b_4921_afa0_872e2349f1583777acba extends com.amazon.milan.compiler.flink.runtime.InstantExtractorEventTimeAssigner[com.amazon.milan.samples.DateValueRecord, Product](java.time.Duration.ofSeconds(5, 0)) {
  override def getInstant(r: com.amazon.milan.samples.DateValueRecord): java.time.Instant = {
    r.dateTime
  }
}


class ProcessAllWindowFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f extends com.amazon.milan.compiler.flink.runtime.TimeWindowProcessAllWindowFunction[Tuple1[Double], com.amazon.milan.compiler.flink.types.ArrayRecord] {
  override def getOutput(windowStart: java.time.Instant, result: Tuple1[Double]): com.amazon.milan.compiler.flink.types.ArrayRecord = {
    val t = {
      Tuple2(windowStart, result._1)
    }
  
    new com.amazon.milan.compiler.flink.types.ArrayRecord(Array(t._1, t._2))
  }
}

class AggregateFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_Sum_63208d30
  extends com.amazon.milan.compiler.flink.runtime.InputMappingAggregateFunctionWrapper[com.amazon.milan.samples.DateValueRecord, Double, Option[Double], Double](
    new com.amazon.milan.compiler.flink.runtime.BuiltinAggregateFunctions.Sum[Double](org.apache.flink.api.scala.createTypeInformation[Double])) {

  override def mapInput(r: com.amazon.milan.samples.DateValueRecord): Double = {
    r.value
  }
}

class AggregateFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_d52ebeb5
  extends com.amazon.milan.compiler.flink.runtime.MultiAggregateFunction1[com.amazon.milan.samples.DateValueRecord, Product, Option[Double], Double](
    new AggregateFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_Sum_63208d30())

class MapFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_TupleExtractor0d67739e extends com.amazon.milan.compiler.flink.runtime.ArrayRecordToTupleMapFunction[Tuple2[java.time.Instant, Double], Product](
  new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple2[java.time.Instant, Double]](Array(org.apache.flink.api.scala.createTypeInformation[java.time.Instant], org.apache.flink.api.scala.createTypeInformation[Double])),
  new com.amazon.milan.compiler.flink.types.NoneTypeInformation) {

  override def getTuple(record: com.amazon.milan.compiler.flink.types.ArrayRecord): Tuple2[java.time.Instant, Double] = {
    Tuple2[java.time.Instant, Double](
      record.productElement(0).asInstanceOf[java.time.Instant],
      record.productElement(1).asInstanceOf[Double]
    )
  }
}

class TimeWindow extends com.amazon.milan.compiler.flink.runtime.MilanApplicationBase {
  override def hasCycles: Boolean =
    false

  override def buildFlinkApplication(env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment): Unit = {
    val stream_fb77893b_caf3_4ffe_8184_27f1a8092473_typeinfo_2e28167a = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.DateValueRecord]
    val stream_fb77893b_caf3_4ffe_8184_27f1a8092473_values_a9f373c4 = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.DateValueRecord]("[{\"dateTime\":1637931505.908308000,\"value\":1.0,\"recordId\":\"a632f238-38c7-4715-9dbc-d5e823632a42\"},{\"dateTime\":1637931506.908308000,\"value\":1.0,\"recordId\":\"13211b4d-2986-48e3-b401-8004663bf7b4\"},{\"dateTime\":1637931507.908308000,\"value\":1.0,\"recordId\":\"ba76f2d6-171b-4f6d-a6d1-3652cfe8043d\"},{\"dateTime\":1637931508.908308000,\"value\":1.0,\"recordId\":\"e6f0c3d2-2f7b-4aa6-b175-5704ac22dcc3\"},{\"dateTime\":1637931509.908308000,\"value\":1.0,\"recordId\":\"3ed1f4f7-5f0b-4791-b54a-859297cf5a21\"},{\"dateTime\":1637931510.908308000,\"value\":1.0,\"recordId\":\"e1b26f34-5258-47fb-ac62-e3ad90cb8566\"},{\"dateTime\":1637931511.908308000,\"value\":1.0,\"recordId\":\"9efb99f5-6bdd-4158-be05-67e170dbae6c\"},{\"dateTime\":1637931512.908308000,\"value\":1.0,\"recordId\":\"d081cbac-85df-4d5a-a718-5669223da20e\"},{\"dateTime\":1637931513.908308000,\"value\":1.0,\"recordId\":\"fcf96529-8867-4810-b642-0cd2c4048af8\"},{\"dateTime\":1637931514.908308000,\"value\":1.0,\"recordId\":\"b689a8cc-3b1d-4a55-8ae5-c138b3f87576\"}]")
    val stream_fb77893b_caf3_4ffe_8184_27f1a8092473_records_2fe73d3d = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.DateValueRecord](
      env,
      stream_fb77893b_caf3_4ffe_8184_27f1a8092473_values_a9f373c4,
      false,
      stream_fb77893b_caf3_4ffe_8184_27f1a8092473_typeinfo_2e28167a)
    val stream_fb77893b_caf3_4ffe_8184_27f1a8092473 = stream_fb77893b_caf3_4ffe_8184_27f1a8092473_records_2fe73d3d.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.DateValueRecord](stream_fb77893b_caf3_4ffe_8184_27f1a8092473_typeinfo_2e28167a))
    
    val stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_timeAssigner_469f00eb = new TimeAssigner_e98a8fa3_8f7b_4921_afa0_872e2349f1583777acba()
    val stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_withEventTime_ = stream_fb77893b_caf3_4ffe_8184_27f1a8092473.assignTimestampsAndWatermarks(stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_timeAssigner_469f00eb)
    
    val stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_windowAssigner_b10d8423 = org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.of(5000, java.util.concurrent.TimeUnit.MILLISECONDS), org.apache.flink.streaming.api.windowing.time.Time.of(1000, java.util.concurrent.TimeUnit.MILLISECONDS), org.apache.flink.streaming.api.windowing.time.Time.of(0, java.util.concurrent.TimeUnit.MILLISECONDS))
    val stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_trigger_6998c068 = new com.amazon.milan.compiler.flink.runtime.RecordWrapperEveryElementTrigger[com.amazon.milan.samples.DateValueRecord, Product, org.apache.flink.streaming.api.windowing.windows.TimeWindow]()
    val stream_e98a8fa3_8f7b_4921_afa0_872e2349f158 = stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_withEventTime_
      .windowAll(stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_windowAssigner_b10d8423)
      .trigger(stream_e98a8fa3_8f7b_4921_afa0_872e2349f158_trigger_6998c068)
    
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_processWindowFunction_81b8afd5 = new ProcessAllWindowFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f()
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_aggregateFunction_fb69b4fb = new AggregateFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_d52ebeb5()
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_accumulatorTypeInfo_f945c546 = new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Option[Double]]](Array(new com.amazon.milan.compiler.flink.types.ParameterizedTypeInfo[Option[Double]](org.apache.flink.api.scala.createTypeInformation[Option[Double]], List(org.apache.flink.api.scala.createTypeInformation[Double]))))
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_intermediateOutputTypeInfo_523a67fa = new com.amazon.milan.compiler.flink.types.ParameterizedTypeInfo[com.amazon.milan.compiler.flink.types.AggregatorOutputRecord[Tuple1[Double]]](org.apache.flink.api.common.typeinfo.TypeInformation.of(classOf[com.amazon.milan.compiler.flink.types.AggregatorOutputRecord[Tuple1[Double]]]), List(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Double]](Array(org.apache.flink.api.scala.createTypeInformation[Double]))))
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_outputTypeInfo_7d8541ff = new com.amazon.milan.compiler.flink.types.RecordWrapperTypeInformation[com.amazon.milan.compiler.flink.types.ArrayRecord, Product](new com.amazon.milan.compiler.flink.types.ArrayRecordTypeInformation(Array(com.amazon.milan.compiler.flink.types.FieldTypeInformation("windowStart", org.apache.flink.api.scala.createTypeInformation[java.time.Instant]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("sum", org.apache.flink.api.scala.createTypeInformation[Double]))), new com.amazon.milan.compiler.flink.types.NoneTypeInformation)
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f = stream_e98a8fa3_8f7b_4921_afa0_872e2349f158.aggregate(
      stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_aggregateFunction_fb69b4fb,
      stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_processWindowFunction_81b8afd5,
      stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_accumulatorTypeInfo_f945c546,
      stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_intermediateOutputTypeInfo_523a67fa,
      stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_outputTypeInfo_7d8541ff)
    
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_tupleExtractorMapFunction_78172bbf = new MapFunction_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_TupleExtractor0d67739e
    val stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_tuples_c66af3c2 = stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f.map(stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_tupleExtractorMapFunction_78172bbf)
    
    stream_c6aecad7_60e0_4bb1_8f10_c980f66b6a8f_tuples_c66af3c2.print()
  }
}

object TimeWindow {
  def main(args: Array[String]): Unit = {
    new TimeWindow().execute(args)
  }
}