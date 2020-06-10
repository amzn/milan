package com.amazon.milan.program

import com.amazon.milan.Id
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


trait GroupingExpression extends SingleInputStreamExpression {
  val expr: FunctionDef

  /**
   * Gets the record type of the input stream that is being grouped.
   */
  @JsonIgnore
  def getInputRecordType: TypeDescriptor[_] = {
    source match {
      case g: GroupingExpression => g.getInputRecordType
      case s: StreamExpression => s.recordType
      case _ => throw new InvalidProgramException("The source of a grouping expression must be another grouping expression or a data stream.")
    }
  }
}

object GroupingExpression {
  def unapply(arg: GroupingExpression): Option[(Tree, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * An expression representing a group-by operation.
 *
 * @param source The input to the group-by operation.
 * @param expr   A function that defines how group keys are extracted from input records.
 */
@JsonSerialize
@JsonDeserialize
class GroupBy(val source: Tree,
              val expr: FunctionDef,
              val nodeId: String,
              val nodeName: String) extends GroupingExpression {

  def this(source: Tree, expr: FunctionDef, nodeId: String) {
    this(source, expr, nodeId, nodeId)
  }

  def this(source: Tree, expr: FunctionDef) {
    this(source, expr, Id.newId())
  }

  def this(source: Tree,
           expr: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, expr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new GroupBy(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new GroupBy(
      children(0),
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.expr})"

  override def equals(obj: Any): Boolean = obj match {
    case GroupBy(s, e) => this.source.equals(s) && this.expr.equals(e)
    case _ => false
  }
}

object GroupBy {
  def apply(source: Tree, expr: FunctionDef): GroupBy = new GroupBy(source, expr)

  def unapply(arg: GroupBy): Option[(Tree, FunctionDef)] = Some((arg.source, arg.expr))
}


trait WindowExpression extends GroupingExpression

object WindowExpression {
  def unapply(arg: WindowExpression): Option[(Tree, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * Defines windows over the latest records to arrive on a stream.
 *
 * @param source     The input stream to the window operation.
 * @param windowSize The number of records in the window.
 *                   If the input is a grouped stream, this is the number of records per group in the window.
 */
@JsonSerialize
@JsonDeserialize
class SlidingRecordWindow(val source: Tree,
                          val windowSize: Int,
                          val nodeId: String,
                          val nodeName: String) extends SingleInputStreamExpression {
  def this(source: Tree, windowSize: Int, nodeId: String) {
    this(source, windowSize, nodeId, nodeId)
  }

  def this(source: Tree, windowSize: Int) {
    this(source, windowSize, Id.newId())
  }

  def this(source: Tree, windowSize: Int, nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(source, windowSize, nodeId, nodeName)
    this.tpe = resultType
  }

  override def getChildren: Iterable[Tree] = Seq(this.source)

  override def replaceChildren(children: List[Tree]): Tree =
    new SlidingRecordWindow(children(0), this.windowSize, this.nodeId, this.nodeName, this.tpe)

  override def withNameAndId(name: String, id: String): StreamExpression =
    new SlidingRecordWindow(this.source, this.windowSize, name, id, this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.windowSize})"

  override def equals(obj: Any): Boolean = obj match {
    case SlidingRecordWindow(s, sz) => this.source.equals(s) && this.windowSize == sz
    case _ => false
  }
}

object SlidingRecordWindow {
  def unapply(arg: SlidingRecordWindow): Option[(Tree, Int)] = Some((arg.source, arg.windowSize))
}


/**
 * Trait that identifies time windows.
 */
trait TimeWindowExpression extends WindowExpression {
  /**
   * The length of a window.
   */
  val size: Duration

  /**
   * By default windows are aligned with the epoch, 1970-01-01 00:00:00 UTC.
   * This offset shifts the window alignment to the specified duration after the epoch.
   */
  val offset: Duration
}

object TimeWindowExpression {
  def unapply(arg: TimeWindowExpression): Option[(Tree, FunctionDef, Duration, Duration)] = Some((arg.source, arg.expr, arg.size, arg.offset))
}


/**
 * An expression representing a tumbling window operation.
 *
 * @param source The input to the tumbling window operation.
 * @param period The period of the window.
 * @param offset By default windows are aligned with the epoch, 1970-01-01.
 *               This offset shifts the window alignment to the specified duration after the epoch.
 */
@JsonSerialize
@JsonDeserialize
class TumblingWindow(val source: Tree,
                     val dateExtractor: FunctionDef,
                     val period: Duration,
                     val offset: Duration,
                     val nodeId: String,
                     val nodeName: String) extends TimeWindowExpression {

  val expr: FunctionDef = this.dateExtractor
  val size: Duration = this.period

  def this(source: Tree, dateExtractor: FunctionDef, period: Duration, offset: Duration, nodeId: String) {
    this(source, dateExtractor, period, offset, nodeId, nodeId)
  }

  def this(source: Tree, dateExtractor: FunctionDef, period: Duration, offset: Duration) {
    this(source, dateExtractor, period, offset, Id.newId())
  }

  def this(source: Tree,
           dateExtractor: FunctionDef,
           period: Duration,
           offset: Duration,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, dateExtractor, period, offset, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new TumblingWindow(this.source, this.dateExtractor, this.period, this.offset, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.dateExtractor, this.period, this.offset)

  override def replaceChildren(children: List[Tree]): Tree =
    new TumblingWindow(
      children(0),
      children(1).asInstanceOf[FunctionDef],
      children(2).asInstanceOf[Duration],
      children(3).asInstanceOf[Duration],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.dateExtractor}, ${this.period}, ${this.offset})"

  override def equals(obj: Any): Boolean = obj match {
    case TumblingWindow(s, e, p, o) => this.source.equals(s) && this.dateExtractor.equals(e) && this.period.equals(p) && this.offset.equals(o)
    case _ => false
  }
}

object TumblingWindow {
  def apply(source: Tree, dateExtractor: FunctionDef, period: Duration, offset: Duration): TumblingWindow =
    new TumblingWindow(source, dateExtractor, period, offset)

  def unapply(arg: TumblingWindow): Option[(Tree, FunctionDef, Duration, Duration)] =
    Some((arg.source, arg.dateExtractor, arg.period, arg.offset))
}


/**
 * An expression representing a sliding window operation.
 *
 * @param source The input stream to the sliding window operation.
 * @param size   The length of a window.
 * @param slide  The distance (in time) between window start times.
 * @param offset By default windows are aligned with the epoch, 1970-01-01.
 *               This offset shifts the window alignment to the specified duration after the epoch.
 */
@JsonSerialize
@JsonDeserialize
class SlidingWindow(val source: Tree,
                    val dateExtractor: FunctionDef,
                    val size: Duration,
                    val slide: Duration,
                    val offset: Duration,
                    val nodeId: String,
                    val nodeName: String) extends TimeWindowExpression {

  val expr: FunctionDef = this.dateExtractor

  def this(source: Tree, dateExtractor: FunctionDef, size: Duration, slide: Duration, offset: Duration, nodeId: String) {
    this(source, dateExtractor, size, slide, offset, nodeId, nodeId)
  }

  def this(source: Tree, dateExtractor: FunctionDef, size: Duration, slide: Duration, offset: Duration) {
    this(source, dateExtractor, size, slide, offset, Id.newId())
  }

  def this(source: Tree,
           dateExtractor: FunctionDef,
           size: Duration,
           slide: Duration,
           offset: Duration,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, dateExtractor, size, slide, offset, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new SlidingWindow(this.source, this.dateExtractor, this.size, this.slide, this.offset, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.dateExtractor, this.size, this.slide, this.offset)

  override def replaceChildren(children: List[Tree]): Tree =
    new SlidingWindow(
      children(0),
      children(1).asInstanceOf[FunctionDef],
      children(2).asInstanceOf[Duration],
      children(3).asInstanceOf[Duration],
      children(4).asInstanceOf[Duration],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.dateExtractor}, ${this.size}, ${this.slide}, ${this.offset})"

  override def equals(obj: Any): Boolean = obj match {
    case SlidingWindow(so, de, sz, sl, of) =>
      this.source.equals(so) &&
        this.dateExtractor.equals(de) &&
        this.size.equals(sz) &&
        this.slide.equals(sl) &&
        this.offset.equals(of)

    case _ =>
      false
  }
}

object SlidingWindow {
  def apply(source: Tree, dateExtractor: FunctionDef, size: Duration, slide: Duration, offset: Duration): SlidingWindow =
    new SlidingWindow(source, dateExtractor, size, slide, offset)

  def unapply(arg: SlidingWindow): Option[(Tree, FunctionDef, Duration, Duration, Duration)] =
    Some((arg.source, arg.dateExtractor, arg.size, arg.slide, arg.offset))
}
