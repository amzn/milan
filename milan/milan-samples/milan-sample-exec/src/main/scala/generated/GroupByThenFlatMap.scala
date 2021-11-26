package com.amazon.milan.samples.exec



class MapFunction_grouped_KeyAssigner_7e569bfa
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

class ScanOperation_1f0fd831_34d3_4498_8795_17714508c6c6_8e7fd9c9
  extends com.amazon.milan.compiler.flink.runtime.AssociativeScanOperation[com.amazon.milan.samples.KeyValueRecord, Int, com.amazon.milan.samples.KeyValueRecord](
  org.apache.flink.api.scala.createTypeInformation[Int],
  org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]) {

  override def getInitialState(numeric: Numeric[Int]): Int =
    numeric.zero

  override def getArg(r: com.amazon.milan.samples.KeyValueRecord): Int = {
    r.value
  }

  override def getOutput(r: com.amazon.milan.samples.KeyValueRecord, sum: Int): com.amazon.milan.samples.KeyValueRecord = {
    com.amazon.milan.samples.KeyValueRecord.apply(r.key, sum)
  }

  override def add(numeric: Numeric[Int], arg1: Int, arg2: Int): Int = {
    numeric.plus(arg1, arg2)
  }

}

class ProcessFunction_grouped_ScanOperationcdaab15f
  extends com.amazon.milan.compiler.flink.runtime.ScanOperationKeyedProcessFunction[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int], Int, com.amazon.milan.samples.KeyValueRecord](
  new ScanOperation_1f0fd831_34d3_4498_8795_17714508c6c6_8e7fd9c9(),
  new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))

class GroupByFlatMap extends com.amazon.milan.compiler.flink.runtime.MilanApplicationBase {
  override def hasCycles: Boolean =
    false

  override def buildFlinkApplication(env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment): Unit = {
    val stream_input_typeinfo_66ae8a2f = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_input_values_e554b09f = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":1,\"value\":1,\"recordId\":\"06c747da-6627-43f8-bf2d-ce662d224e62\"},{\"key\":1,\"value\":2,\"recordId\":\"773f1497-ad30-40c0-8083-f8a8ffc0506c\"},{\"key\":2,\"value\":5,\"recordId\":\"2a2bb9b2-8184-408e-9e07-b1026c5c824b\"},{\"key\":3,\"value\":6,\"recordId\":\"cd1f00d9-5c25-4db3-b0cb-2cc8fdb2855c\"},{\"key\":2,\"value\":3,\"recordId\":\"5457d19e-c25d-4334-8460-e87ff1349d9f\"},{\"key\":3,\"value\":1,\"recordId\":\"c6216326-0565-4eb6-ba24-974ca10f5733\"}]")
    val stream_input_records_5c98990c = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_input_values_e554b09f,
      false,
      stream_input_typeinfo_66ae8a2f)
    val stream_input = stream_input_records_5c98990c.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_input_typeinfo_66ae8a2f))
    
    val stream_grouped_keyAssignerMapFunction_3c0de761 = new MapFunction_grouped_KeyAssigner_7e569bfa()
    val stream_grouped_mappedWithKeys_bd3cc383 = stream_input.map(stream_grouped_keyAssignerMapFunction_3c0de761)
    
    val stream_grouped_keySelector_b29f89ee = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))
    val stream_grouped_keyed_17176985 = stream_grouped_mappedWithKeys_bd3cc383.keyBy(stream_grouped_keySelector_b29f89ee, stream_grouped_keySelector_b29f89ee.getKeyType)
    
    val stream_1f0fd831_34d3_4498_8795_17714508c6c6_processFunction_dcd77405 = new ProcessFunction_grouped_ScanOperationcdaab15f()
    val stream_1f0fd831_34d3_4498_8795_17714508c6c6_unkeyed = stream_grouped_keyed_17176985.process(stream_1f0fd831_34d3_4498_8795_17714508c6c6_processFunction_dcd77405, stream_1f0fd831_34d3_4498_8795_17714508c6c6_processFunction_dcd77405.getProducedType)
    
    val stream_1f0fd831_34d3_4498_8795_17714508c6c6_keyed_keySelector_5b6a38c4 = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))
    val stream_1f0fd831_34d3_4498_8795_17714508c6c6_keyed_keyed_c7cab477 = stream_1f0fd831_34d3_4498_8795_17714508c6c6_unkeyed.keyBy(stream_1f0fd831_34d3_4498_8795_17714508c6c6_keyed_keySelector_5b6a38c4, stream_1f0fd831_34d3_4498_8795_17714508c6c6_keyed_keySelector_5b6a38c4.getKeyType)
    
    val stream_output_recordTypeInfo_f78063ae = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_output_keyTypeInfo_0cad3105 = new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int]))
    val stream_output_flatMap_7252f2bc = new com.amazon.milan.compiler.flink.runtime.IdentityFlatMapFunction[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](stream_output_recordTypeInfo_f78063ae, stream_output_keyTypeInfo_0cad3105)
    val stream_output = stream_1f0fd831_34d3_4498_8795_17714508c6c6_keyed_keyed_c7cab477.flatMap(stream_output_flatMap_7252f2bc)
    
    stream_output.print()
  }
}

object GroupByFlatMap {
  def main(args: Array[String]): Unit = {
    new GroupByFlatMap().execute(args)
  }
}