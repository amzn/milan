package com.amazon.milan.samples.exec

import com.amazon.milan.compiler.flink.runtime.implicits._

class TimeAssigner_85b3963f_8660_4186_ab03_2c6c146a8152bfe7518e extends com.amazon.milan.compiler.flink.runtime.InstantExtractorEventTimeAssigner[com.amazon.milan.samples.DateValueRecord, Product](java.time.Duration.ofSeconds(5, 0)) {
  override def getInstant(r: com.amazon.milan.samples.DateValueRecord): java.time.Instant = {
    r.dateTime
  }
}


class ProcessAllWindowFunction_c20612ed_01bc_4621_8692_a5377682507e extends com.amazon.milan.compiler.flink.runtime.TimeWindowProcessAllWindowFunction[Tuple1[Double], com.amazon.milan.compiler.flink.types.ArrayRecord] {
  override def getOutput(windowStart: java.time.Instant, result: Tuple1[Double]): com.amazon.milan.compiler.flink.types.ArrayRecord = {
    val t = {
      Tuple2(windowStart, result._1)
    }
  
    new com.amazon.milan.compiler.flink.types.ArrayRecord(Array(t._1, t._2))
  }
}

class AggregateFunction_c20612ed_01bc_4621_8692_a5377682507e_Sum_27fbdb7f
  extends com.amazon.milan.compiler.flink.runtime.InputMappingAggregateFunctionWrapper[com.amazon.milan.samples.DateValueRecord, Double, Option[Double], Double](
    new com.amazon.milan.compiler.flink.runtime.BuiltinAggregateFunctions.Sum[Double](org.apache.flink.api.scala.createTypeInformation[Double])) {

  override def mapInput(r: com.amazon.milan.samples.DateValueRecord): Double = {
    r.value
  }
}

class AggregateFunction_c20612ed_01bc_4621_8692_a5377682507e_29dd11f6
  extends com.amazon.milan.compiler.flink.runtime.MultiAggregateFunction1[com.amazon.milan.samples.DateValueRecord, Product, Option[Double], Double](
    new AggregateFunction_c20612ed_01bc_4621_8692_a5377682507e_Sum_27fbdb7f())

class MapFunction_c20612ed_01bc_4621_8692_a5377682507e_TupleExtractora3ce9a22 extends com.amazon.milan.compiler.flink.runtime.ArrayRecordToTupleMapFunction[Tuple2[java.time.Instant, Double], Product](
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
    val stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df_typeinfo_f615362e = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.DateValueRecord]
    val stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df_values_d6af0340 = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.DateValueRecord]("[{\"dateTime\":1602856862.648836000,\"value\":1.0,\"recordId\":\"5a419965-99df-487a-a410-255b31549fe5\"},{\"dateTime\":1602856863.648836000,\"value\":1.0,\"recordId\":\"943558f7-3d88-4a42-b4ec-8d6d47af50f4\"},{\"dateTime\":1602856864.648836000,\"value\":1.0,\"recordId\":\"33ce623e-0f78-463e-9d24-598b5726a959\"},{\"dateTime\":1602856865.648836000,\"value\":1.0,\"recordId\":\"962af277-e7bb-4d81-8df9-1d7ae67c0ad7\"},{\"dateTime\":1602856866.648836000,\"value\":1.0,\"recordId\":\"f135a213-e221-4222-b341-24323b706b27\"},{\"dateTime\":1602856867.648836000,\"value\":1.0,\"recordId\":\"3871b199-ed1e-4a1a-b9ea-44f06ebac611\"},{\"dateTime\":1602856868.648836000,\"value\":1.0,\"recordId\":\"c3456e6e-3c28-4321-ba8a-1f9380806cbf\"},{\"dateTime\":1602856869.648836000,\"value\":1.0,\"recordId\":\"eb6cfa37-b8af-4768-ab97-5be9a4673fb6\"},{\"dateTime\":1602856870.648836000,\"value\":1.0,\"recordId\":\"eccd815a-d61d-42af-9968-8e47d38b155d\"},{\"dateTime\":1602856871.648836000,\"value\":1.0,\"recordId\":\"1442aae6-57fc-4ec2-98ba-aa8c35ac436a\"}]")
    val stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df_records_a53d3576 = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.DateValueRecord](
      env,
      stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df_values_d6af0340,
      false,
      stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df_typeinfo_f615362e)
    val stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df = stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df_records_a53d3576.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.DateValueRecord](stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df_typeinfo_f615362e))
    
    val stream_85b3963f_8660_4186_ab03_2c6c146a8152_timeAssigner_a46892fe = new TimeAssigner_85b3963f_8660_4186_ab03_2c6c146a8152bfe7518e()
    val stream_85b3963f_8660_4186_ab03_2c6c146a8152_withEventTime_ = stream_d3b2062a_ba41_40f2_bc65_c09c3d2716df.assignTimestampsAndWatermarks(stream_85b3963f_8660_4186_ab03_2c6c146a8152_timeAssigner_a46892fe)
    
    val stream_85b3963f_8660_4186_ab03_2c6c146a8152_windowAssigner_a2e5d87a = org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.of(5000, java.util.concurrent.TimeUnit.MILLISECONDS), org.apache.flink.streaming.api.windowing.time.Time.of(1000, java.util.concurrent.TimeUnit.MILLISECONDS), org.apache.flink.streaming.api.windowing.time.Time.of(0, java.util.concurrent.TimeUnit.MILLISECONDS))
    val stream_85b3963f_8660_4186_ab03_2c6c146a8152_trigger_09ff5b86 = new com.amazon.milan.compiler.flink.runtime.RecordWrapperEveryElementTrigger[com.amazon.milan.samples.DateValueRecord, Product, org.apache.flink.streaming.api.windowing.windows.TimeWindow]()
    val stream_85b3963f_8660_4186_ab03_2c6c146a8152 = stream_85b3963f_8660_4186_ab03_2c6c146a8152_withEventTime_
      .windowAll(stream_85b3963f_8660_4186_ab03_2c6c146a8152_windowAssigner_a2e5d87a)
      .trigger(stream_85b3963f_8660_4186_ab03_2c6c146a8152_trigger_09ff5b86)
    
    val stream_c20612ed_01bc_4621_8692_a5377682507e_processWindowFunction_8d3e79a9 = new ProcessAllWindowFunction_c20612ed_01bc_4621_8692_a5377682507e()
    val stream_c20612ed_01bc_4621_8692_a5377682507e_aggregateFunction_d10818a5 = new AggregateFunction_c20612ed_01bc_4621_8692_a5377682507e_29dd11f6()
    val stream_c20612ed_01bc_4621_8692_a5377682507e_accumulatorTypeInfo_9413d024 = new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Option[Double]]](Array(new com.amazon.milan.compiler.flink.types.ParameterizedTypeInfo[Option[Double]](org.apache.flink.api.scala.createTypeInformation[Option[Double]], List(org.apache.flink.api.scala.createTypeInformation[Double]))))
    val stream_c20612ed_01bc_4621_8692_a5377682507e_intermediateOutputTypeInfo_b7fcd027 = new com.amazon.milan.compiler.flink.types.ParameterizedTypeInfo[com.amazon.milan.compiler.flink.types.AggregatorOutputRecord[Tuple1[Double]]](org.apache.flink.api.common.typeinfo.TypeInformation.of(classOf[com.amazon.milan.compiler.flink.types.AggregatorOutputRecord[Tuple1[Double]]]), List(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Double]](Array(org.apache.flink.api.scala.createTypeInformation[Double]))))
    val stream_c20612ed_01bc_4621_8692_a5377682507e_outputTypeInfo_96600e2b = new com.amazon.milan.compiler.flink.types.RecordWrapperTypeInformation[com.amazon.milan.compiler.flink.types.ArrayRecord, Product](new com.amazon.milan.compiler.flink.types.ArrayRecordTypeInformation(Array(com.amazon.milan.compiler.flink.types.FieldTypeInformation("windowStart", org.apache.flink.api.scala.createTypeInformation[java.time.Instant]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("sum", org.apache.flink.api.scala.createTypeInformation[Double]))), new com.amazon.milan.compiler.flink.types.NoneTypeInformation)
    val stream_c20612ed_01bc_4621_8692_a5377682507e = stream_85b3963f_8660_4186_ab03_2c6c146a8152.aggregate(
      stream_c20612ed_01bc_4621_8692_a5377682507e_aggregateFunction_d10818a5,
      stream_c20612ed_01bc_4621_8692_a5377682507e_processWindowFunction_8d3e79a9,
      stream_c20612ed_01bc_4621_8692_a5377682507e_accumulatorTypeInfo_9413d024,
      stream_c20612ed_01bc_4621_8692_a5377682507e_intermediateOutputTypeInfo_b7fcd027,
      stream_c20612ed_01bc_4621_8692_a5377682507e_outputTypeInfo_96600e2b)
    
    val stream_c20612ed_01bc_4621_8692_a5377682507e_tupleExtractorMapFunction_1f28996e = new MapFunction_c20612ed_01bc_4621_8692_a5377682507e_TupleExtractora3ce9a22
    val stream_c20612ed_01bc_4621_8692_a5377682507e_tuples_bbdb9ee5 = stream_c20612ed_01bc_4621_8692_a5377682507e.map(stream_c20612ed_01bc_4621_8692_a5377682507e_tupleExtractorMapFunction_1f28996e)
    
    stream_c20612ed_01bc_4621_8692_a5377682507e_tuples_bbdb9ee5.print()
  }
}

object TimeWindow {
  def main(args: Array[String]): Unit = {
    new TimeWindow().execute(args)
  }
}