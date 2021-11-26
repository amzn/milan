package com.amazon.milan.compiler.scala.event.operators


trait FoldOperation[TIn, TKey, TOut] {
  def fold(items: TraversableOnce[TIn], key: TKey): TOut
}


class FoldScanOperation[TIn, TKey, TState, TOut](scanOperation: ScanOperation[TIn, TKey, TState, TOut])
  extends FoldOperation[TIn, TKey, TOut] {

  override def fold(items: TraversableOnce[TIn], key: TKey): TOut = {
    val (_, Some(finalOutput)) = {
      items.foldLeft((this.scanOperation.initialState, Option.empty[TOut]))((state, item) => {
        val (opState, _) = state
        val (newState, output) = this.scanOperation.process(opState, item, key)
        (newState, Some(output))
      })
    }

    finalOutput
  }
}
