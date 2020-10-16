package com.amazon.milan.samples.exec



class MapFunction_grouped_KeyAssigner_ceb0710a
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

class ScanOperation_c86077c1_3e06_4117_9d74_1f8a817e9035_17a22059
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

class ProcessFunction_grouped_ScanOperation9da5a0ed
  extends com.amazon.milan.compiler.flink.runtime.ScanOperationKeyedProcessFunction[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int], Int, com.amazon.milan.samples.KeyValueRecord](
  new ScanOperation_c86077c1_3e06_4117_9d74_1f8a817e9035_17a22059(),
  new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))

class GroupByFlatMap extends com.amazon.milan.compiler.flink.runtime.MilanApplicationBase {
  override def hasCycles: Boolean =
    false

  override def buildFlinkApplication(env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment): Unit = {
    val stream_input_typeinfo_16c5eca0 = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_input_values_5bfa8770 = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":1,\"value\":1,\"recordId\":\"9cfe3b65-0019-49d2-9a69-fe2ba402ee3c\"},{\"key\":1,\"value\":2,\"recordId\":\"439fc041-9105-43af-ba78-febc5c927229\"},{\"key\":2,\"value\":5,\"recordId\":\"3cb0c8c8-5311-4054-83b0-5ffcdd118371\"},{\"key\":3,\"value\":6,\"recordId\":\"37b852c1-7c76-4b5b-9a84-8172c6225174\"},{\"key\":2,\"value\":3,\"recordId\":\"f70f0a74-da48-4284-b10a-e98e77ebdace\"},{\"key\":3,\"value\":1,\"recordId\":\"013b30dc-59a5-4c51-9fc7-206fb851551f\"}]")
    val stream_input_records_57174b58 = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_input_values_5bfa8770,
      false,
      stream_input_typeinfo_16c5eca0)
    val stream_input = stream_input_records_57174b58.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_input_typeinfo_16c5eca0))
    
    val stream_grouped_keyAssignerMapFunction_a532f91b = new MapFunction_grouped_KeyAssigner_ceb0710a()
    val stream_grouped_mappedWithKeys_02d769b5 = stream_input.map(stream_grouped_keyAssignerMapFunction_a532f91b)
    
    val stream_grouped_keySelector_88362d3b = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))
    val stream_grouped_keyed_41053b7c = stream_grouped_mappedWithKeys_02d769b5.keyBy(stream_grouped_keySelector_88362d3b, stream_grouped_keySelector_88362d3b.getKeyType)
    
    val stream_c86077c1_3e06_4117_9d74_1f8a817e9035_processFunction_52b410da = new ProcessFunction_grouped_ScanOperation9da5a0ed()
    val stream_c86077c1_3e06_4117_9d74_1f8a817e9035_unkeyed = stream_grouped_keyed_41053b7c.process(stream_c86077c1_3e06_4117_9d74_1f8a817e9035_processFunction_52b410da, stream_c86077c1_3e06_4117_9d74_1f8a817e9035_processFunction_52b410da.getProducedType)
    
    val stream_c86077c1_3e06_4117_9d74_1f8a817e9035_keyed_keySelector_93e87fd4 = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))
    val stream_c86077c1_3e06_4117_9d74_1f8a817e9035_keyed_keyed_457b5aee = stream_c86077c1_3e06_4117_9d74_1f8a817e9035_unkeyed.keyBy(stream_c86077c1_3e06_4117_9d74_1f8a817e9035_keyed_keySelector_93e87fd4, stream_c86077c1_3e06_4117_9d74_1f8a817e9035_keyed_keySelector_93e87fd4.getKeyType)
    
    val stream_output_recordTypeInfo_4b6e41d6 = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_output_keyTypeInfo_bcde908e = new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int]))
    val stream_output_flatMap_0370db18 = new com.amazon.milan.compiler.flink.runtime.IdentityFlatMapFunction[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](stream_output_recordTypeInfo_4b6e41d6, stream_output_keyTypeInfo_bcde908e)
    val stream_output = stream_c86077c1_3e06_4117_9d74_1f8a817e9035_keyed_keyed_457b5aee.flatMap(stream_output_flatMap_0370db18)
    
    stream_output.print()
  }
}

object GroupByFlatMap {
  def main(args: Array[String]): Unit = {
    new GroupByFlatMap().execute(args)
  }
}