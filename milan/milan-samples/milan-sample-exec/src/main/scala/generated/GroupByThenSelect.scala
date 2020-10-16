package com.amazon.milan.samples.exec

import com.amazon.milan.compiler.flink.runtime.implicits._

class MapFunction_7597eede_cd92_4625_80cb_81e2086d5a5e_KeyAssigner_2195eba4
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

class AggregateFunction_output_Sum_98d21b39
  extends com.amazon.milan.compiler.flink.runtime.InputMappingAggregateFunctionWrapper[com.amazon.milan.samples.KeyValueRecord, Int, Option[Int], Int](
    new com.amazon.milan.compiler.flink.runtime.BuiltinAggregateFunctions.Sum[Int](org.apache.flink.api.scala.createTypeInformation[Int])) {

  override def mapInput(r: com.amazon.milan.samples.KeyValueRecord): Int = {
    r.value
  }
}

class AggregateFunction_output_0681e7cd
  extends com.amazon.milan.compiler.flink.runtime.MultiAggregateFunction1[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int], Option[Int], Int](
    new AggregateFunction_output_Sum_98d21b39())

class MapFunction_91c4a472_436b_40f7_97fd_002bd22f09c7_TupleExtractor2337c6f1 extends com.amazon.milan.compiler.flink.runtime.ArrayRecordToTupleMapFunction[Tuple2[Int, Int], Product](
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
    val stream_input_typeinfo_2286a4be = org.apache.flink.api.scala.createTypeInformation[com.amazon.milan.samples.KeyValueRecord]
    val stream_input_values_b7e2ee78 = com.amazon.milan.compiler.flink.runtime.RuntimeUtil.loadJsonList[com.amazon.milan.samples.KeyValueRecord]("[{\"key\":1,\"value\":1,\"recordId\":\"f8b1e5af-f39a-4958-8186-6eacb1520b6a\"},{\"key\":1,\"value\":2,\"recordId\":\"96288c8b-7020-4b4b-a503-f14017464570\"},{\"key\":2,\"value\":5,\"recordId\":\"757ea66d-c831-4082-880f-2c4f98446528\"},{\"key\":3,\"value\":6,\"recordId\":\"311546ed-cda4-43f8-9cb5-952e56dbd4a2\"},{\"key\":2,\"value\":3,\"recordId\":\"eedb1754-6bc6-4fb9-aa72-fd9bd47f7f08\"},{\"key\":3,\"value\":1,\"recordId\":\"a7fa1ca7-a5a5-497d-881d-b94aaa93ec45\"}]")
    val stream_input_records_ba16957d = com.amazon.milan.compiler.flink.runtime.DataSourceUtil.addListDataSource[com.amazon.milan.samples.KeyValueRecord](
      env,
      stream_input_values_b7e2ee78,
      false,
      stream_input_typeinfo_2286a4be)
    val stream_input = stream_input_records_ba16957d.map(new com.amazon.milan.compiler.flink.runtime.WrapRecordsMapFunction[com.amazon.milan.samples.KeyValueRecord](stream_input_typeinfo_2286a4be))
    
    val stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyAssignerMapFunction_94e6fc4e = new MapFunction_7597eede_cd92_4625_80cb_81e2086d5a5e_KeyAssigner_2195eba4()
    val stream_7597eede_cd92_4625_80cb_81e2086d5a5e_mappedWithKeys_eb57cbad = stream_input.map(stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyAssignerMapFunction_94e6fc4e)
    
    val stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keySelector_0efc2d56 = new com.amazon.milan.compiler.flink.runtime.RecordWrapperKeySelector[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int]](
      new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Int]](Array(org.apache.flink.api.scala.createTypeInformation[Int])))
    val stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741 = stream_7597eede_cd92_4625_80cb_81e2086d5a5e_mappedWithKeys_eb57cbad.keyBy(stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keySelector_0efc2d56, stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keySelector_0efc2d56.getKeyType)
    
    val stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741_window_bb90a21d = org.apache.flink.streaming.api.windowing.assigners.GlobalWindows.create()
    val stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741_trigger_7c8ac92b = new com.amazon.milan.compiler.flink.runtime.RecordWrapperEveryElementTrigger[com.amazon.milan.samples.KeyValueRecord, Tuple1[Int], org.apache.flink.streaming.api.windowing.windows.GlobalWindow]
    val stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741_windowed_a4b120a5 = stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741
      .window(stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741_window_bb90a21d)
      .trigger(stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741_trigger_7c8ac92b)
    
    val stream_output_processWindowFunction_6c64c6cf = new ProcessWindowFunction_output()
    val stream_output_aggregateFunction_e003ce1b = new AggregateFunction_output_0681e7cd()
    val stream_output_accumulatorTypeInfo_9a474429 = new com.amazon.milan.compiler.flink.types.ScalaTupleTypeInformation[Tuple1[Option[Int]]](Array(new com.amazon.milan.compiler.flink.types.ParameterizedTypeInfo[Option[Int]](org.apache.flink.api.scala.createTypeInformation[Option[Int]], List(org.apache.flink.api.scala.createTypeInformation[Int]))))
    val stream_output_outputTypeInfo_c7dd9804 = new com.amazon.milan.compiler.flink.types.RecordWrapperTypeInformation[com.amazon.milan.compiler.flink.types.ArrayRecord, Product](new com.amazon.milan.compiler.flink.types.ArrayRecordTypeInformation(Array(com.amazon.milan.compiler.flink.types.FieldTypeInformation("key", org.apache.flink.api.scala.createTypeInformation[Int]), com.amazon.milan.compiler.flink.types.FieldTypeInformation("sum", org.apache.flink.api.scala.createTypeInformation[Int]))), new com.amazon.milan.compiler.flink.types.NoneTypeInformation)
    val stream_output = stream_7597eede_cd92_4625_80cb_81e2086d5a5e_keyed_f7368741_windowed_a4b120a5.aggregate(
      stream_output_aggregateFunction_e003ce1b,
      stream_output_processWindowFunction_6c64c6cf,
      stream_output_accumulatorTypeInfo_9a474429,
      stream_output_aggregateFunction_e003ce1b.getProducedType,
      stream_output_outputTypeInfo_c7dd9804)
    
    val stream_91c4a472_436b_40f7_97fd_002bd22f09c7_tupleExtractorMapFunction_f2772c76 = new MapFunction_91c4a472_436b_40f7_97fd_002bd22f09c7_TupleExtractor2337c6f1
    val stream_91c4a472_436b_40f7_97fd_002bd22f09c7_tuples_8fbb76d8 = stream_output.map(stream_91c4a472_436b_40f7_97fd_002bd22f09c7_tupleExtractorMapFunction_f2772c76)
    
    stream_91c4a472_436b_40f7_97fd_002bd22f09c7_tuples_8fbb76d8.print()
  }
}

object GroupBySelect {
  def main(args: Array[String]): Unit = {
    new GroupBySelect().execute(args)
  }
}