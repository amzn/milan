package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.TypeUtil
import org.apache.flink.api.common.typeinfo.TypeInformation


abstract class ArgCompareKeyedProcessFunction[T >: Null, TKey >: Null <: Product, TArg](recordTypeInformation: TypeInformation[T],
                                                                                        keyTypeInformation: TypeInformation[TKey],
                                                                                        argTypeInformation: TypeInformation[TArg])
  extends ScanKeyedProcessFunction[T, TKey, Option[TArg], T](None, keyTypeInformation, TypeUtil.createOptionTypeInfo(argTypeInformation), recordTypeInformation) {

  protected def getArg(value: T): TArg

  protected def greaterThan(arg1: TArg, arg2: TArg): Boolean

  override protected def process(state: Option[TArg], key: TKey, value: T): (Option[TArg], Option[T]) = {
    val valueArg = this.getArg(value)

    state match {
      case None =>
        (Some(valueArg), Some(value))

      case Some(stateArg) =>
        if (this.greaterThan(valueArg, stateArg)) {
          (Some(valueArg), Some(value))
        }
        else {
          (state, None)
        }
    }
  }
}


abstract class ArgCompareProcessFunction[T >: Null, TKey >: Null <: Product, TArg](recordTypeInformation: TypeInformation[T],
                                                                                   keyTypeInformation: TypeInformation[TKey],
                                                                                   argTypeInformation: TypeInformation[TArg])
  extends ScanProcessFunction[T, TKey, Option[TArg], T](None, keyTypeInformation, TypeUtil.createOptionTypeInfo(argTypeInformation), recordTypeInformation) {

  protected def getArg(value: T): TArg

  protected def greaterThan(arg1: TArg, arg2: TArg): Boolean

  override protected def process(state: Option[TArg], value: T): (Option[TArg], Option[T]) = {
    val valueArg = this.getArg(value)

    state match {
      case None =>
        (Some(valueArg), Some(value))

      case Some(stateArg) =>
        if (this.greaterThan(valueArg, stateArg)) {
          (Some(valueArg), Some(value))
        }
        else {
          (state, None)
        }
    }
  }
}