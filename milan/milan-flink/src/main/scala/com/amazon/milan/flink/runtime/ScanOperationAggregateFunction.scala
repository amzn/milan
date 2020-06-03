package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.TypeUtil
import com.amazon.milan.flink.types.{OptionTypeInformation, RecordWrapper, RecordWrapperTypeInformation}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.slf4j.LoggerFactory


/**
 * Flink [[AggregateFunction]] that hosts a Milan [[ScanOperation]].
 *
 * @param scanOperation         A scan operation.
 * @param inputTypeInformation  [[TypeInformation]] for the scan operation's input type.
 * @param stateTypeInformation  [[TypeInformation]] for the scan operation's state type.
 * @param outputTypeInformation [[TypeInformation]] for the scan operation's output type.
 * @tparam TIn    The input type.
 * @tparam TState The state type.
 * @tparam TOut   The output type.
 */
class ScanOperationAggregateFunction[TIn >: Null, TKey >: Null <: Product, TState, TOut](scanOperation: ScanOperation[TIn, TState, TOut],
                                                                                         inputTypeInformation: TypeInformation[TIn],
                                                                                         keyTypeInformation: TypeInformation[TKey],
                                                                                         stateTypeInformation: TypeInformation[TState],
                                                                                         outputTypeInformation: TypeInformation[TOut])
  extends AggregateFunction[RecordWrapper[TIn, TKey], (Option[(TIn, TState)], TKey, TState, Option[TOut]), RecordWrapper[Option[TOut], TKey]]
    with ResultTypeQueryable[RecordWrapper[Option[TOut], TKey]] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * The definition of the scan operation is (State, In) => (State, Out).
   * It assumes that records are processed in sequentially.
   * However, Flink's AggregateFunction supports parallel processing and merging of states, so we need to do some
   * trickery to allow us to convert a Milan scan operation into an AggregateFunction.
   *
   * The first trick implements AggregateFunction.getOutput, which takes an accumulator and produces an output record.
   * Scan doesn't do that, so instead we produce the output record inside AggregateFunction.add() which is the only
   * place we see the input record. add() puts the output record into the accumulator, that way getOutput just returns
   * whatever output record arrives in the accumulator.
   *
   * The second trick implements AggregateFunction.merge, which takes two accumulators and produces a new accumulator
   * containing the merged information. Unfortunately this requires us to require Milan's ScanOperation to provide
   * a mergeStates function. But even if we can merge states, we don't know how to merge output records. To solve this,
   * we put into the accumulator the previous input record and the previous state. Then merging accumulators means
   * merging the two previous states, then processing the two previous output records to produce a new output and a new
   * merged state.
   */

  def getAccumulatorType: TypeInformation[(Option[(TIn, TState)], TKey, TState, Option[TOut])] =
    TypeUtil.createTupleTypeInfo[(Option[(TIn, TState)], TKey, TState, Option[TOut])](
      OptionTypeInformation.wrap(TypeUtil.createTupleTypeInfo[(TIn, TState)](this.inputTypeInformation, this.stateTypeInformation)),
      this.keyTypeInformation,
      this.stateTypeInformation,
      OptionTypeInformation.wrap(this.outputTypeInformation))

  override def createAccumulator(): (Option[(TIn, TState)], TKey, TState, Option[TOut]) =
    (None, null, this.scanOperation.getInitialState, None)

  override def add(record: RecordWrapper[TIn, TKey],
                   acc: (Option[(TIn, TState)], TKey, TState, Option[TOut])): (Option[(TIn, TState)], TKey, TState, Option[TOut]) = {
    val (_, _, state, _) = acc
    val (newState, output) = this.scanOperation.process(state, record.value)
    (Some(record.value, state), record.key, newState, output)
  }

  override def merge(accA: (Option[(TIn, TState)], TKey, TState, Option[TOut]),
                     accB: (Option[(TIn, TState)], TKey, TState, Option[TOut])): (Option[(TIn, TState)], TKey, TState, Option[TOut]) = {
    val (prevA, key, _, _) = accA
    val (prevB, _, _, _) = accB

    (prevA, prevB) match {
      case (None, None) =>
        // Both of these accumulators are empty, we can just output one of them.
        accA

      case (Some(_), None) =>
        accA

      case (None, Some(_)) =>
        accB

      case (Some((prevInputA, prevStateA)), Some((prevInputB, prevStateB))) =>
        // Log this because if it happens a lot we need to think about making this process more efficient.
        this.logger.info("Merging states and reprocessing previous inputs.")

        // Here's where it gets interesting.
        // First merge the previous states, then re-process the previous inputs for each branch to produce
        // a new merged output.
        val mergedState = this.scanOperation.mergeStates(prevStateA, prevStateB)
        val (newStateA, _) = this.scanOperation.process(mergedState, prevInputA)
        val (newStateB, outputB) = this.scanOperation.process(newStateA, prevInputB)
        (Some(prevInputB, newStateA), key, newStateB, outputB)
    }
  }

  override def getResult(acc: (Option[(TIn, TState)], TKey, TState, Option[TOut])): RecordWrapper[Option[TOut], TKey] = {
    val (_, key, _, output) = acc
    RecordWrapper.wrap(output, key, 0L)
  }

  override def getProducedType: TypeInformation[RecordWrapper[Option[TOut], TKey]] =
    RecordWrapperTypeInformation.wrap(
      OptionTypeInformation.wrap(this.outputTypeInformation),
      this.keyTypeInformation)
}
