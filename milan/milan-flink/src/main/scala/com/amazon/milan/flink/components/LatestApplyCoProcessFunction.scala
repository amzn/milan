package com.amazon.milan.flink.components

import java.time.Instant

import com.amazon.milan.flink.TypeUtil
import com.amazon.milan.flink.compiler.internal._
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.types.{LineageRecord, RecordWithLineage}
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._


/**
 * Flink [[CoProcessFunction]] that implements left.leftJoin(right.latestBy(...)).apply(...).
 *
 * @param dateExtractor A function that extracts timestamps from records on the right stream.
 * @param keyExtractor  A function that extracts keys from records on the right stream.
 * @param applyFunction A function that produces an output record given a left record and the current cache of right records.
 */
class LatestApplyCoProcessFunction[TLeft, TRight, TKey, TOut](leftInputType: TypeDescriptor[TLeft],
                                                              rightInputType: TypeDescriptor[TRight],
                                                              outputType: TypeDescriptor[TOut],
                                                              dateExtractor: SerializableFunction[TRight, Instant],
                                                              keyExtractor: SerializableFunction[TRight, TKey],
                                                              applyFunction: SerializableFunction2[TLeft, Iterable[TRight], TOut],
                                                              rightTypeInformation: TypeInformation[TRight],
                                                              keyTypeInformation: TypeInformation[TKey],
                                                              outputTypeInformation: TypeInformation[TOut],
                                                              lineageRecordFactory: JoinLineageRecordFactory)
  extends CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]
    with ResultTypeQueryable[RecordWithLineage[TOut]] {

  @transient private lazy val getLeftRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.leftInputType)
  @transient private lazy val getRightRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.rightInputType)
  @transient private lazy val getOutputRecordId = RecordIdExtractorFactory.getRecordIdExtractor(this.outputType)
  @transient private lazy val canProduceLineage = getLeftRecordId.isDefined && getRightRecordId.isDefined && getOutputRecordId.isDefined

  @transient private var rightCache: MapState[TKey, Tuple2[TRight, Instant]] = _

  private val producedType = new RecordWithLineageTypeInformation[TOut](this.outputTypeInformation)

  def this(dateExtractorFunc: FunctionDef,
           keyExtractorFunc: FunctionDef,
           applyFunc: FunctionDef,
           leftType: TypeDescriptor[TLeft],
           rightType: TypeDescriptor[TRight],
           rightTypeInformation: TypeInformation[TRight],
           keyTypeInformation: TypeInformation[TKey],
           outputTypeInformation: TypeInformation[TOut],
           lineageRecordFactory: JoinLineageRecordFactory) {
    this(
      leftType,
      rightType,
      applyFunc.tpe.getRecordType.asInstanceOf[TypeDescriptor[TOut]],
      new RuntimeCompiledFunction[TRight, Instant](rightType, dateExtractorFunc),
      new RuntimeCompiledFunction[TRight, TKey](rightType, keyExtractorFunc),
      new RuntimeCompiledFunction2[TLeft, Iterable[TRight], TOut](leftType, TypeDescriptor.iterableOf[TRight](rightType), applyFunc),
      rightTypeInformation,
      keyTypeInformation,
      outputTypeInformation,
      lineageRecordFactory)
  }

  override def processElement1(leftValue: TLeft,
                               context: CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]#Context,
                               collector: Collector[RecordWithLineage[TOut]]): Unit = {
    // Get the right values from the cache where the predicate returned true.
    val rightValues = this.rightCache.values().asScala.map(_.f0)

    val applyResult = this.applyFunction(leftValue, rightValues)
    val lineageRecord =
      if (this.canProduceLineage) {
        this.createLineageRecord(this.getOutputRecordId.get(applyResult), leftValue)
      }
      else {
        null
      }
    val outputRecord = new RecordWithLineage[TOut](applyResult, lineageRecord)
    collector.collect(outputRecord)
  }

  override def processElement2(rightValue: TRight,
                               context: CoProcessFunction[TLeft, TRight, RecordWithLineage[TOut]]#Context,
                               collector: Collector[RecordWithLineage[TOut]]): Unit = {
    // Update the collection of right values.
    // Right values arriving does not trigger any computation or output.
    val key = this.keyExtractor(rightValue)
    val newTimeStamp = this.dateExtractor(rightValue)

    // Check whether the value we just got is newer than the value in the cache.
    // If it is then we'll apply the update logic.
    val isNewer = !this.rightCache.contains(key) || newTimeStamp.isAfter(this.rightCache.get(key).f1)

    if (isNewer) {
      this.rightCache.put(key, new Tuple2(rightValue, newTimeStamp))
    }
  }

  override def open(parameters: Configuration): Unit = {
    val valueTypeInfo = TypeUtil.createTupleTypeInfo[Tuple2[TRight, Instant]](
      this.rightTypeInformation,
      createTypeInformation[Instant])

    val rightValuesDescriptor = new MapStateDescriptor[TKey, Tuple2[TRight, Instant]]("rightCache", this.keyTypeInformation, valueTypeInfo)

    this.rightCache = this.getRuntimeContext.getMapState(rightValuesDescriptor)
  }

  override def getProducedType: TypeInformation[RecordWithLineage[TOut]] = this.producedType

  private def createLineageRecord(outputRecordId: String, leftRecord: TLeft): LineageRecord = {
    // TODO: Figure out how lineage tracking works for functions like this.
    val sourceRecords = Seq(this.lineageRecordFactory.createLeftRecordPointer(this.getLeftRecordId.get(leftRecord)))
    this.lineageRecordFactory.createLineageRecord(outputRecordId, sourceRecords)
  }
}
