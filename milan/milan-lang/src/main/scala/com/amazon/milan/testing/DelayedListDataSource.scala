package com.amazon.milan.testing

import java.time.Duration

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


class ItemOrDelay[T](val item: Option[T], val delay: Option[Duration]) extends Serializable {
  def this() {
    this(None, None)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: ItemOrDelay[T] => this.item.equals(o.item) && this.delay.equals(o.delay)
    case _ => false
  }

  override def toString: String = s"ItemOrDelay(${this.item}, ${this.delay})"
}

object ItemOrDelay {
  def item[T](item: T): ItemOrDelay[T] = new ItemOrDelay(Some(item), None)

  def delay[T](delay: Duration): ItemOrDelay[T] = new ItemOrDelay[T](None, Some(delay))
}

object DelayedListDataSource {
  def builder[T](): DelayedListSourceBuilder[T] =
    new DelayedListSourceBuilder[T](List.empty)
}


@JsonSerialize
@JsonDeserialize
class DelayedListDataSource[T: TypeDescriptor](val values: List[ItemOrDelay[T]], val runForever: Boolean = false)
  extends DataSource[T] {

  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]

  }

  override def equals(obj: Any): Boolean = obj match {
    case o: DelayedListDataSource[_] =>
      this.getGenericArguments.equals(o.getGenericArguments) &&
        this.values.equals(o.values)

    case _ =>
      false
  }
}


class DelayedListSourceBuilder[T: TypeDescriptor](val values: List[ItemOrDelay[T]]) {
  def add(item: T): DelayedListSourceBuilder[T] =
    new DelayedListSourceBuilder[T](this.values :+ ItemOrDelay.item(item))

  def wait(delay: Duration): DelayedListSourceBuilder[T] =
    new DelayedListSourceBuilder[T](this.values :+ ItemOrDelay.delay(delay))

  def build(runForever: Boolean = false): DelayedListDataSource[T] =
    new DelayedListDataSource[T](this.values, runForever)
}
