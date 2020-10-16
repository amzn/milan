package com.amazon.milan.samples.exec



class MapFunction_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_KeyAssigner_5d57939a
  extends com.amazon.milan.compiler.flink.runtime.ModifyRecordKeyMapFunction[com.amazon.milan.samples.KeyValueRecord, Product, Tuple1[Tuple1[Int]]](
    org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord],
    new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int]))))) {

  private def getKey(l: com.amazon.milan.samples.KeyValueRecord): Tuple1[Int] = {
    Tuple1(l.key)
  }

  private def combineKeys(key: Product, newElem: Tuple1[Int]): Tuple1[Tuple1[Int]] = {
    Tuple1(newElem)
  }

  override def getNewKey(value: com.amazon.milan.samples.KeyValueRecord, key: Product): Tuple1[Tuple1[Int]] = {
    val newKey = this.getKey(value)
    this.combineKeys(key, newKey)
  }
}

class MapFunction_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_KeyAssigner_7fab208e
  extends com.amazon.milan.compiler.flink.runtime.ModifyRecordKeyMapFunction[com.amazon.milan.samples.KeyValueRecord, Product, Tuple1[Tuple1[Int]]](
    org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord],
    new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int]))))) {

  private def getKey(r: com.amazon.milan.samples.KeyValueRecord): Tuple1[Int] = {
    Tuple1(r.key)
  }

  private def combineKeys(key: Product, newElem: Tuple1[Int]): Tuple1[Tuple1[Int]] = {
    Tuple1(newElem)
  }

  override def getNewKey(value: com.amazon.milan.samples.KeyValueRecord, key: Product): Tuple1[Tuple1[Int]] = {
    val newKey = this.getKey(value)
    this.combineKeys(key, newKey)
  }
}

class RecordIdExtractor_1a479965 extends com.amazon.milan.compiler.flink.runtime.RecordIdExtractor[com.amazon.milan.samples.KeyValueRecord] {
  override def canExtractRecordId: Boolean = true

  override def apply(record: com.amazon.milan.samples.KeyValueRecord): String =
    record.recordId.toString()
}

class RecordIdExtractor_135a5114 extends com.amazon.milan.compiler.flink.runtime.RecordIdExtractor[com.amazon.milan.samples.KeyValueRecord] {
  override def canExtractRecordId: Boolean = true

  override def apply(record: com.amazon.milan.samples.KeyValueRecord): String =
    record.recordId.toString()
}

class CoProcessFunction_FullJoin_outputf38e5017
  extends com.amazon.milan.compiler.flink.runtime.FullJoinKeyedCoProcessFunction[com.amazon.milan.samples.KeyValueRecord, com.amazon.milan.samples.KeyValueRecord, Tuple1[Tuple1[Int]], com.amazon.milan.compiler.flink.types.ArrayRecord](
    org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord],
    org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord],
    new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))),
    new com.amazon.milan.compiler.flink.types.ArrayRecordTypeInformation(Array(com.amazon.milan.compiler.flink.types.FieldTypeInformation("key", org.apache.flink.api.scala.createTypeInformation[Int]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("left", org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("right", org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]))),
    new RecordIdExtractor_1a479965(),
    new RecordIdExtractor_135a5114(),
    new com.amazon.milan.compiler.flink.generator.ArrayRecordIdExtractor(),
    new com.amazon.milan.compiler.flink.internal.ComponentJoinLineageRecordFactory("a1ca1d8a-791a-411f-a8a7-5ae1d7d13cf5", "cb9c38e4-580f-4d3e-9198-6cef0ccfee32", "1b8ee4ea-220f-402e-8973-83e9f6c46c4b", "09ea99a9-7476-48c0-9daa-4fe816d61060", "09ea99a9-7476-48c0-9daa-4fe816d61060", new com.amazon.milan.SemanticVersion(0, 0, 0, null, null)),
    new org.apache.flink.util.OutputTag[com.amazon.milan.types.LineageRecord]("lineage", org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.types.LineageRecord]),
    new com.amazon.milan.compiler.flink.metrics.OperatorMetricFactory("", "output")) {

  override def map(l: com.amazon.milan.samples.KeyValueRecord, r: com.amazon.milan.samples.KeyValueRecord): com.amazon.milan.compiler.flink.types.ArrayRecord = {
    val t = {
      Tuple3(if ((l == null)) { r.key } else { l.key }, l, r)
    }
  
    new com.amazon.milan.compiler.flink.types.ArrayRecord(Array(t._1, t._2, t._3))
  }

  override def postCondition(left: com.amazon.milan.samples.KeyValueRecord, right: com.amazon.milan.samples.KeyValueRecord): Boolean = true
}

class MapFunction_output_TupleExtractor74d39788 extends com.amazon.milan.compiler.flink.runtime.ArrayRecordToTupleMapFunction[Tuple3[Int, com.amazon.milan.samples.KeyValueRecord, com.amazon.milan.samples.KeyValueRecord], Tuple1[Tuple1[Int]]](
  new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple3[Int, com.amazon.milan.samples.KeyValueRecord, com.amazon.milan.samples.KeyValueRecord]](Array(org.apache.flink.api.scala.createTypeInformation[Int], org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord], org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord])),
  new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int]))))) {

  override def getTuple(record: com.amazon.milan.compiler.flink.types.ArrayRecord): Tuple3[Int, com.amazon.milan.samples.KeyValueRecord, com.amazon.milan.samples.KeyValueRecord] = {
    Tuple3[Int, com.amazon.milan.samples.KeyValueRecord, com.amazon.milan.samples.KeyValueRecord](
      record.productElement(0).asInstanceOf[Int],
      record.productElement(1).asInstanceOf[com.amazon.milan.samples.KeyValueRecord],
      record.productElement(2).asInstanceOf[com.amazon.milan.samples.KeyValueRecord]
    )
  }
}

class FullJoin extends com.amazon.milan.compiler.flink.runtime.MilanApplicationBase {
  override def hasCycles: Boolean =
    false

  override def buildFlinkApplication(env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment): Unit = {
    val stream_left_typeinfo_bb69cadb = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_left_values_e22b83c9 = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":1,\"value\":1,\"recordId\":\"b59b1ff7-cb22-453a-9770-1a92c552ebfa\"},{\"key\":1,\"value\":2,\"recordId\":\"d677dc38-b512-49cd-95ba-5697b641769c\"},{\"key\":2,\"value\":3,\"recordId\":\"bcf725ad-70a4-440d-8823-b571a8da3ecb\"},{\"key\":2,\"value\":4,\"recordId\":\"00ab9d66-d6b3-44b0-9d12-e78b2e2612af\"}]")
    val stream_left_records_6dd4756a = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_left_values_e22b83c9,
      false,
      stream_left_typeinfo_bb69cadb)
    val stream_left = stream_left_records_6dd4756a.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_left_typeinfo_bb69cadb))
    
    val stream_right_typeinfo_0b5ae40e = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_right_values_9f03b047 = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":2,\"value\":1,\"recordId\":\"92be86d6-431d-4974-9d8a-33e3fab5a13f\"},{\"key\":1,\"value\":3,\"recordId\":\"67a4791d-de26-44dc-97d2-f50ebc8136fa\"},{\"key\":2,\"value\":2,\"recordId\":\"264211de-62f2-4136-b878-5ae187191bd2\"},{\"key\":1,\"value\":5,\"recordId\":\"e2f9d957-6efb-475d-b3ac-9c88ffac3ac7\"}]")
    val stream_right_records_561509e2 = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_right_values_9f03b047,
      false,
      stream_right_typeinfo_0b5ae40e)
    val stream_right = stream_right_records_561509e2.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_right_typeinfo_0b5ae40e))
    
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_keyAssignerMapFunction_4ffe3c4b = new MapFunction_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_KeyAssigner_5d57939a()
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_mappedWithKeys_990cf5b7 = stream_left.map(stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_keyAssignerMapFunction_4ffe3c4b)
    
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_keySelector_1c00486a = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Tuple1[Int]]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))))
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_keyed_671b3ac7 = stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_mappedWithKeys_990cf5b7.keyBy(stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_keySelector_1c00486a, stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_keySelector_1c00486a.getKeyType)
    
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_keyAssignerMapFunction_9997c79e = new MapFunction_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_KeyAssigner_7fab208e()
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_mappedWithKeys_18c18cf0 = stream_right.map(stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_keyAssignerMapFunction_9997c79e)
    
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_keySelector_c76aebe0 = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Tuple1[Int]]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))))
    val stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_keyed_ee77d673 = stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_mappedWithKeys_18c18cf0.keyBy(stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_keySelector_c76aebe0, stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_keySelector_c76aebe0.getKeyType)
    
    val connected_ee2516d1_fd48_4f44_a7de_68c1515afa4b_9562a869 = stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_left_input_keyed_671b3ac7.connect(stream_ee2516d1_fd48_4f44_a7de_68c1515afa4b_right_input_keyed_ee77d673)
    
    val stream_output_coProcessFunction_de38db04 = new CoProcessFunction_FullJoin_outputf38e5017()
    val stream_output_recordTypeInfo_bed7f7c4 = stream_output_coProcessFunction_de38db04.getProducedType
    val stream_output = connected_ee2516d1_fd48_4f44_a7de_68c1515afa4b_9562a869.process(stream_output_coProcessFunction_de38db04, stream_output_recordTypeInfo_bed7f7c4)
    
    val stream_output_tupleExtractorMapFunction_4fcdea62 = new MapFunction_output_TupleExtractor74d39788
    val stream_output_tuples_38ccc76c = stream_output.map(stream_output_tupleExtractorMapFunction_4fcdea62)
    
    stream_output_tuples_38ccc76c.print()
  }
}

object FullJoin {
  def main(args: Array[String]): Unit = {
    new FullJoin().execute(args)
  }
}