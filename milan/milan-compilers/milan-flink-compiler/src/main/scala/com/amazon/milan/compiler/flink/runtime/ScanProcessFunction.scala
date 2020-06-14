package com.amazon.milan.compiler.flink.runtime

import com.amazon.milan.compiler.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector


/**
 * Base class for scan operations on non-keyed streams.
 */
abstract class ScanProcessFunction[TIn >: Null, TKey >: Null <: Product, TState, TOut >: Null](initialState: TState,
                                                                                               keyTypeInformation: TypeInformation[TKey],
                                                                                               stateTypeInformation: TypeInformation[TState],
                                                                                               outputTypeInformation: TypeInformation[TOut])
  extends ProcessFunction[RecordWrapper[TIn, TKey], RecordWrapper[TOut, TKey]]
    with CheckpointedFunction
    with ResultTypeQueryable[RecordWrapper[TOut, TKey]] {

  private var state: TState = this.initialState
  @transient private var checkpointedState: ListState[TState] = _
  @transient private var sequenceNumberHelper: OperatorSequenceNumberHelper = _

  protected def process(state: TState, value: TIn): (TState, Option[TOut])

  override def processElement(record: RecordWrapper[TIn, TKey],
                              context: ProcessFunction[RecordWrapper[TIn, TKey], RecordWrapper[TOut, TKey]]#Context,
                              collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    val (newState, output) = this.process(this.state, record.value)
    this.state = newState

    output match {
      case Some(value) =>
        val outputRecord = RecordWrapper.wrap(value, record.key, this.sequenceNumberHelper.increment())
        collector.collect(outputRecord)

      case _ =>
        ()
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stateDescriptor = new ListStateDescriptor[TState]("state", this.stateTypeInformation)
    this.checkpointedState = context.getOperatorStateStore.getListState(stateDescriptor)

    this.sequenceNumberHelper = OperatorSequenceNumberHelper.create(context)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    this.checkpointedState.clear()
    this.checkpointedState.add(this.state)

    this.sequenceNumberHelper.snapshotState()
  }

  override def getProducedType: TypeInformation[RecordWrapper[TOut, TKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInformation, this.keyTypeInformation)
}


abstract class ScanKeyedProcessFunction[TIn >: Null, TKey >: Null <: Product, TState, TOut >: Null](initialState: TState,
                                                                                                    keyTypeInformation: TypeInformation[TKey],
                                                                                                    stateTypeInformation: TypeInformation[TState],
                                                                                                    outputTypeInformation: TypeInformation[TOut])
  extends KeyedProcessFunction[TKey, RecordWrapper[TIn, TKey], RecordWrapper[TOut, TKey]]
    with ResultTypeQueryable[RecordWrapper[TOut, TKey]] {

  @transient private var state: ValueState[TState] = _
  @transient private var sequenceNumberHelper: SequenceNumberHelper = _

  protected def process(state: TState, key: TKey, value: TIn): (TState, Option[TOut])

  override def processElement(record: RecordWrapper[TIn, TKey],
                              context: KeyedProcessFunction[TKey, RecordWrapper[TIn, TKey], RecordWrapper[TOut, TKey]]#Context,
                              collector: Collector[RecordWrapper[TOut, TKey]]): Unit = {
    val currentState =
      if (this.state.value() == null) {
        this.initialState
      }
      else {
        this.state.value()
      }

    val (newState, output) = this.process(currentState, context.getCurrentKey, record.value)
    this.state.update(newState)

    output match {
      case Some(value) =>
        val outputRecord = RecordWrapper.wrap[TOut, TKey](value, context.getCurrentKey, this.sequenceNumberHelper.increment())
        collector.collect(outputRecord)

      case _ =>
        ()
    }
  }

  override def open(parameters: Configuration): Unit = {
    val stateDescriptor = new ValueStateDescriptor[TState]("state", this.stateTypeInformation)
    this.state = this.getRuntimeContext.getState(stateDescriptor)

    this.sequenceNumberHelper = new SequenceNumberHelper(this.getRuntimeContext)
  }

  override def getProducedType: TypeInformation[RecordWrapper[TOut, TKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInformation, this.keyTypeInformation)
}
