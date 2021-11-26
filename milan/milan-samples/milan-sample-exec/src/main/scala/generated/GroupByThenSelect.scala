package com.amazon.milan.samples.exec

import com.amazon.milan.compiler.flink.runtime.implicits._

class MapFunction_368f68c6_2528_4065_b9e3_5d76ab9eadc9_KeyAssigner_f022454d
  extends com.amazon.milan.compiler.flink.runtime.ModifyRecordKeyMapFunction[com.amazon.milan.samples.KeyValueRecord, Product, Tuple1[Int]](
    org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord],
    new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int]))) {

  private def getKey(r: com.amazon.milan.samples.KeyValueRecord): Int = {
    r.key
  }

  private def combineKeys(key: Product, newElem: Int): Tuple1[Int] = {
    Tuple1(newElem)
  }

  override def getNewKey(value: com.amazon.milan.samples.KeyValueRecord, key: Product): Tuple1[Int] = {
    val newKey = this.getKey(value)
    this.combineKeys(key, newKey)
  }
}

class ProcessWindowFunction_output extends com.amazon.milan.compiler.flink.runtime.GroupByProcessWindowFunction[Tuple1[Int], Tuple1[Int], Int, com.amazon.milan.compiler.flink.types.ArrayRecord] {
  override def getKey(recordKey: Tuple1[Int]): Int = {
    recordKey._1
  }

  override def getOutput(key: Int, result: Tuple1[Int]): com.amazon.milan.compiler.flink.types.ArrayRecord = {
    val t = {
      Tuple2(key, result._1)
    }
  
    new com.amazon.milan.compiler.flink.types.ArrayRecord(Array(t._1, t._2))
  }
}

class AggregateFunction_output_Sum_574933a9
  extends com.amazon.milan.compiler.flink.runtime.InputMappingAggregateFunctionWrapper[com.amazon.milan.samples.KeyValueRecord, Int, Option[Int], Int](
    new com.amazon.milan.compiler.flink.runtime.BuiltinAggregateFunctions.Sum[Int](org.apache.flink.api.scala.createTypeInformation[Int])) {

  override def mapInput(r: com.amazon.milan.samples.KeyValueRecord): Int = {
    r.value
  }
}

class AggregateFunction_output_302e9032
  extends com.amazon.milan.compiler.flink.runtime.MultiAggregateFunction1[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int], Option[Int], Int](
    new AggregateFunction_output_Sum_574933a9())

class MapFunction_b8ae4e1d_6be7_4362_8e3a_d93e44ebf8a7_TupleExtractora195c2b0 extends com.amazon.milan.compiler.flink.runtime.ArrayRecordToTupleMapFunction[Tuple2[Int, Int], Product](
  new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple2[Int, Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int], org.apache.flink.api.scala.createTypeInformation[Int])),
  new com.amazon.milan.compiler.flink.types.NoneTypeInformation) {

  override def getTuple(record: com.amazon.milan.compiler.flink.types.ArrayRecord): Tuple2[Int, Int] = {
    Tuple2[Int, Int](
      record.productElement(0).asInstanceOf[Int],
      record.productElement(1).asInstanceOf[Int]
    )
  }
}

class GroupBySelect extends com.amazon.milan.compiler.flink.runtime.MilanApplicationBase {
  override def hasCycles: Boolean =
    false

  override def buildFlinkApplication(env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment): Unit = {
    val stream_input_typeinfo_dc790c3b = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_input_values_bef6c96b = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":1,\"value\":1,\"recordId\":\"0fb665a6-be8a-4e61-9d08-98f3ab0f6693\"},{\"key\":1,\"value\":2,\"recordId\":\"8d305fd7-9fa6-403d-9ae9-5b85fd2426e8\"},{\"key\":2,\"value\":5,\"recordId\":\"81eb970b-2201-4d26-a148-72b718cb9621\"},{\"key\":3,\"value\":6,\"recordId\":\"621a4526-ef0a-4acb-aebd-d54d5ed88dfe\"},{\"key\":2,\"value\":3,\"recordId\":\"6833bb8a-b566-4e54-bc90-93ab3884b4fc\"},{\"key\":3,\"value\":1,\"recordId\":\"3cd25428-4fa0-410f-b2dc-82d455e719df\"}]")
    val stream_input_records_ad79d6b5 = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_input_values_bef6c96b,
      false,
      stream_input_typeinfo_dc790c3b)
    val stream_input = stream_input_records_ad79d6b5.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_input_typeinfo_dc790c3b))
    
    val stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyAssignerMapFunction_dd22f5ab = new MapFunction_368f68c6_2528_4065_b9e3_5d76ab9eadc9_KeyAssigner_f022454d()
    val stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_mappedWithKeys_e8143299 = stream_input.map(stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyAssignerMapFunction_dd22f5ab)
    
    val stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keySelector_4cebd66b = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))
    val stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629 = stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_mappedWithKeys_e8143299.keyBy(stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keySelector_4cebd66b, stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keySelector_4cebd66b.getKeyType)
    
    val stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629_window_10012720 = org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create()
    val stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629_trigger_e678307f = new com.amazon.milan.compiler.flink.runtime.RecordWrapperEveryElementTrigger[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int], org.apache.flink.streaming.api.windowing.windows.GlobalWindow]
    val stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629_windowed_79945480 = stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629
      .window(stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629_window_10012720)
      .trigger(stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629_trigger_e678307f)
    
    val stream_output_processWindowFunction_f64ddd14 = new ProcessWindowFunction_output()
    val stream_output_aggregateFunction_14ceba25 = new AggregateFunction_output_302e9032()
    val stream_output_accumulatorTypeInfo_e39d74ef = new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Option[Int]]](Array(new com.amazon.milan.compiler.flink.types.ParameterizedTypeInfo[Option[Int]](org.apache.flink.api.scala.createTypeInformation[Option[Int]], List(org.apache.flink.api.scala.createTypeInformation[Int]))))
    val stream_output_outputTypeInfo_8e1edc84 = new com.amazon.milan.compiler.flink.types.RecordWrapperTypeInformation[com.amazon.milan.compiler.flink.types.ArrayRecord, Product](new com.amazon.milan.compiler.flink.types.ArrayRecordTypeInformation(Array(com.amazon.milan.compiler.flink.types.FieldTypeInformation("key", org.apache.flink.api.scala.createTypeInformation[Int]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("sum", org.apache.flink.api.scala.createTypeInformation[Int]))), new com.amazon.milan.compiler.flink.types.NoneTypeInformation)
    val stream_output = stream_368f68c6_2528_4065_b9e3_5d76ab9eadc9_keyed_02251629_windowed_79945480.aggregate(
      stream_output_aggregateFunction_14ceba25,
      stream_output_processWindowFunction_f64ddd14,
      stream_output_accumulatorTypeInfo_e39d74ef,
      stream_output_aggregateFunction_14ceba25.getProducedType,
      stream_output_outputTypeInfo_8e1edc84)
    
    val stream_b8ae4e1d_6be7_4362_8e3a_d93e44ebf8a7_tupleExtractorMapFunction_e359d7cd = new MapFunction_b8ae4e1d_6be7_4362_8e3a_d93e44ebf8a7_TupleExtractora195c2b0
    val stream_b8ae4e1d_6be7_4362_8e3a_d93e44ebf8a7_tuples_d0edc07a = stream_output.map(stream_b8ae4e1d_6be7_4362_8e3a_d93e44ebf8a7_tupleExtractorMapFunction_e359d7cd)
    
    stream_b8ae4e1d_6be7_4362_8e3a_d93e44ebf8a7_tuples_d0edc07a.print()
  }
}

object GroupBySelect {
  def main(args: Array[String]): Unit = {
    new GroupBySelect().execute(args)
  }
}