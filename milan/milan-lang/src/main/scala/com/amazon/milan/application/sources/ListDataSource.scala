package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import scala.language.experimental.macros


/**
 * A [[DataSource]] that provides a fixed set of items.
 *
 * @param values     The items to provide form the source.
 * @param runForever Specifies whether the data source should stop after the last item is collected.
 *                   If false, the data source will idle rather than stop.
 *                   Flink will terminate an application when all of the sources have stopped; setting this to true will
 *                   prevent that.
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize
@JsonDeserialize
class ListDataSource[T: TypeDescriptor](val values: List[T], val runForever: Boolean = false)
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: ListDataSource[_] =>
      this.getGenericArguments.equals(o.getGenericArguments) &&
        this.values.equals(o.values)

    case _ =>
      false
  }
}
