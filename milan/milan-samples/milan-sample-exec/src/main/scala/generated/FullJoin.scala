package com.amazon.milan.samples.exec



class MapFunction_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_KeyAssigner_2fc78e21
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

class MapFunction_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_KeyAssigner_290e55bb
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

class RecordIdExtractor_9e201803 extends com.amazon.milan.compiler.flink.runtime.RecordIdExtractor[com.amazon.milan.samples.KeyValueRecord] {
  override def canExtractRecordId: Boolean = true

  override def apply(record: com.amazon.milan.samples.KeyValueRecord): String =
    record.recordId.toString()
}

class RecordIdExtractor_6f7ca99d extends com.amazon.milan.compiler.flink.runtime.RecordIdExtractor[com.amazon.milan.samples.KeyValueRecord] {
  override def canExtractRecordId: Boolean = true

  override def apply(record: com.amazon.milan.samples.KeyValueRecord): String =
    record.recordId.toString()
}

class CoProcessFunction_FullJoin_output64dd6a4b
  extends com.amazon.milan.compiler.flink.runtime.FullJoinKeyedCoProcessFunction[com.amazon.milan.samples.KeyValueRecord, com.amazon.milan.samples.KeyValueRecord, Tuple1[Tuple1[Int]], com.amazon.milan.compiler.flink.types.ArrayRecord](
    org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord],
    org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord],
    new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))),
    new com.amazon.milan.compiler.flink.types.ArrayRecordTypeInformation(Array(com.amazon.milan.compiler.flink.types.FieldTypeInformation("key", org.apache.flink.api.scala.createTypeInformation[Int]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("left", org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("right", org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]))),
    new RecordIdExtractor_9e201803(),
    new RecordIdExtractor_6f7ca99d(),
    new com.amazon.milan.compiler.flink.generator.ArrayRecordIdExtractor(),
    new com.amazon.milan.compiler.flink.internal.ComponentJoinLineageRecordFactory("c2d15fe0-10f4-4b3d-976e-1f3188b506d2", "990ff2b4-d329-4f99-be21-3d56f612c3f0", "d306964b-58ba-491c-b86f-aeb00f4f8b69", "9bbefab0-463d-48fa-ad4f-83a0dd215fac", "9bbefab0-463d-48fa-ad4f-83a0dd215fac", new com.amazon.milan.SemanticVersion(0, 0, 0, null, null)),
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

class MapFunction_output_TupleExtractor27c3659b extends com.amazon.milan.compiler.flink.runtime.ArrayRecordToTupleMapFunction[Tuple3[Int, com.amazon.milan.samples.KeyValueRecord, com.amazon.milan.samples.KeyValueRecord], Tuple1[Tuple1[Int]]](
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
    val stream_left_typeinfo_415c0136 = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_left_values_ca8ff440 = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":1,\"value\":1,\"recordId\":\"be2a5e35-7829-416d-b36c-8cd36608005b\"},{\"key\":1,\"value\":2,\"recordId\":\"bc8b4afa-fe79-4e0b-ae79-00ebb2780ec4\"},{\"key\":2,\"value\":3,\"recordId\":\"d441765a-8e9d-42fe-98ad-2f0c94782482\"},{\"key\":2,\"value\":4,\"recordId\":\"ae752548-acb6-48d3-9721-513914bbfc98\"}]")
    val stream_left_records_13e2a460 = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_left_values_ca8ff440,
      false,
      stream_left_typeinfo_415c0136)
    val stream_left = stream_left_records_13e2a460.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_left_typeinfo_415c0136))
    
    val stream_right_typeinfo_fd3a091d = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_right_values_203f8a5e = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":2,\"value\":1,\"recordId\":\"f325a77b-e338-4464-a185-01602f8d1628\"},{\"key\":1,\"value\":3,\"recordId\":\"612b6f03-a797-45fc-90bf-90263aa754e0\"},{\"key\":2,\"value\":2,\"recordId\":\"b0a65c25-c7bf-40ac-9364-71e310f4779f\"},{\"key\":1,\"value\":5,\"recordId\":\"ec90bbf7-a56f-47eb-a154-e188508ff005\"}]")
    val stream_right_records_d3b2d4bc = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_right_values_203f8a5e,
      false,
      stream_right_typeinfo_fd3a091d)
    val stream_right = stream_right_records_d3b2d4bc.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_right_typeinfo_fd3a091d))
    
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_keyAssignerMapFunction_904644ee = new MapFunction_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_KeyAssigner_2fc78e21()
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_mappedWithKeys_9eded60f = stream_left.map(stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_keyAssignerMapFunction_904644ee)
    
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_keySelector_518aa04b = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Tuple1[Int]]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))))
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_keyed_f16a6f0b = stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_mappedWithKeys_9eded60f.keyBy(stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_keySelector_518aa04b, stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_keySelector_518aa04b.getKeyType)
    
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_keyAssignerMapFunction_ac657122 = new MapFunction_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_KeyAssigner_290e55bb()
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_mappedWithKeys_b15366fc = stream_right.map(stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_keyAssignerMapFunction_ac657122)
    
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_keySelector_18b6f3b9 = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Tuple1[Int]]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Tuple1[Int]]](Array(new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))))
    val stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_keyed_86697742 = stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_mappedWithKeys_b15366fc.keyBy(stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_keySelector_18b6f3b9, stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_keySelector_18b6f3b9.getKeyType)
    
    val connected_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_f4f32b87 = stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_left_input_keyed_f16a6f0b.connect(stream_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_right_input_keyed_86697742)
    
    val stream_output_coProcessFunction_828b8e5d = new CoProcessFunction_FullJoin_output64dd6a4b()
    val stream_output_recordTypeInfo_b12f166a = stream_output_coProcessFunction_828b8e5d.getProducedType
    val stream_output = connected_bca387d7_3890_4e5e_98c4_45a65b3ca5bb_f4f32b87.process(stream_output_coProcessFunction_828b8e5d, stream_output_recordTypeInfo_b12f166a)
    
    val stream_output_tupleExtractorMapFunction_0e72c369 = new MapFunction_output_TupleExtractor27c3659b
    val stream_output_tuples_7287beda = stream_output.map(stream_output_tupleExtractorMapFunction_0e72c369)
    
    stream_output_tuples_7287beda.print()
  }
}

object FullJoin {
  def main(args: Array[String]): Unit = {
    new FullJoin().execute(args)
  }
}