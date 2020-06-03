package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import scala.language.experimental.macros


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
