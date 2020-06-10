package com.amazon.milan.testing

import java.time.Duration

import com.amazon.milan.application.DataSource
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A union of a record and/or a delay.
 *
 * @param item  An item to produce from the data source.
 * @param delay An amount of time to wait before sending the next item.
 * @tparam T The item type.
 */
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
  /**
   * Creates an [[ItemOrDelay]] containing an item and no delay.
   */
  def item[T](item: T): ItemOrDelay[T] = new ItemOrDelay(Some(item), None)

  /**
   * Creates an [[ItemOrDelay]] containing a delay and no item.
   *
   * @param delay
   * @tparam T
   * @return
   */
  def delay[T](delay: Duration): ItemOrDelay[T] = new ItemOrDelay[T](None, Some(delay))
}

object DelayedListDataSource {
  /**
   * Creates a [[DelayedListSourceBuilder]] that can be used to construct a [[DelayedListDataSource]]
   *
   * @tparam T The item type.
   * @return A [[DelayedListSourceBuilder]] object.
   */
  def builder[T](): DelayedListSourceBuilder[T] =
    new DelayedListSourceBuilder[T](List.empty)
}


/**
 * A [[DataSource]] that produces a list of items with optional delays between sending items.
 *
 * @param values     The items to send, encoded as an item or a delay.
 * @param runForever Whether the source should continue running the the items have all been sent to the program.
 *                   If true, the source will idle until cancelled.
 * @tparam T The type of objects produced by the data source.
 */
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
  /**
   * Adds producing an item as the next thing for the source to do.
   */
  def add(item: T): DelayedListSourceBuilder[T] =
    new DelayedListSourceBuilder[T](this.values :+ ItemOrDelay.item(item))

  /**
   * Adds a delay as the next thing for the source to do.
   */
  def wait(delay: Duration): DelayedListSourceBuilder[T] =
    new DelayedListSourceBuilder[T](this.values :+ ItemOrDelay.delay(delay))

  /**
   * Creates and returns a [[DelayedListDataSource]] containing the items and delays that were added to the builder.
   *
   * @param runForever Whether the source should continue running the the items have all been sent to the program.
   *                   If true, the source will idle until cancelled.
   */
  def build(runForever: Boolean = false): DelayedListDataSource[T] =
    new DelayedListDataSource[T](this.values, runForever)
}
