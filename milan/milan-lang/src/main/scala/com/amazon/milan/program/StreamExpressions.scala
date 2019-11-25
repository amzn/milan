package com.amazon.milan.program

import com.amazon.milan.typeutil.{StreamTypeDescriptor, TypeDescriptor}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.commons.lang.builder.HashCodeBuilder


/**
 * Base trait for all graph expressions.
 */
@JsonSerialize(using = classOf[TreeSerializer])
@JsonDeserialize(using = classOf[TreeDeserializer])
trait GraphNodeExpression extends Tree {
  val nodeId: String
  val nodeName: String

  def withName(name: String): GraphNodeExpression = this.withNameAndId(name, this.nodeId)

  def withId(id: String): GraphNodeExpression = this.withNameAndId(this.nodeName, id)

  def withNameAndId(name: String, id: String): GraphNodeExpression

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)
}


/**
 * Trait for graph expressions that have a single input node.
 */
@JsonSerialize(using = classOf[TreeSerializer])
@JsonDeserialize(using = classOf[TreeDeserializer])
trait SingleInputGraphNodeExpression {
  @JsonIgnore
  def getInput: GraphNodeExpression
}

object SingleInputGraphNodeExpression {
  def unapply(arg: SingleInputGraphNodeExpression): Option[GraphNodeExpression] = Some(arg.getInput)
}


/**
 * Trait for graph expressions that represent actual data streams.
 */
@JsonSerialize(using = classOf[TreeSerializer])
@JsonDeserialize(using = classOf[TreeDeserializer])
trait StreamExpression extends GraphNodeExpression {
  @JsonIgnore
  def streamType: StreamTypeDescriptor = this.tpe.asStream

  @JsonIgnore
  def recordType: TypeDescriptor[_] = this.streamType.recordType
}


/**
 * A reference to a graph node.
 *
 * @param nodeId   The ID of the referenced node.
 * @param nodeName The name of the referenced node.
 */
class Ref(val nodeId: String, val nodeName: String) extends StreamExpression {
  def this(nodeId: String) {
    this(nodeId, nodeId)
  }

  def this(nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression = new Ref(id, name, this.tpe)

  override def toString: String = s"""${this.expressionType}("${this.nodeId}")"""

  override def equals(obj: Any): Boolean = obj match {
    case Ref(id) => this.nodeId.equals(id)
    case _ => false
  }
}

object Ref {
  def apply(nodeId: String): Ref = new Ref(nodeId)

  def unapply(arg: Ref): Option[String] = Some(arg.nodeId)
}


trait MapNodeExpression extends StreamExpression with SingleInputGraphNodeExpression {
  val source: GraphNodeExpression

  override def getInput: GraphNodeExpression = this.source
}

object MapNodeExpression {
  def unapply(arg: MapNodeExpression): Option[GraphNodeExpression] = Some(arg.source)
}


/**
 * An expression representing a map operation that results in a stream of objects.
 *
 * @param source The input to the map operation.
 * @param expr   The function that defines the operation.
 */
class MapRecord(val source: GraphNodeExpression,
                val expr: FunctionDef,
                val nodeId: String = "",
                val nodeName: String = "") extends MapNodeExpression {

  def this(source: GraphNodeExpression,
           expr: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, expr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new MapRecord(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source, expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new MapRecord(
      children(0).asInstanceOf[GraphNodeExpression],
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.expr})"

  override def equals(obj: Any): Boolean = obj match {
    case MapRecord(s, e) => this.source.equals(s) && this.expr.equals(e)
    case _ => false
  }
}

object MapRecord {
  def apply(source: GraphNodeExpression, expr: FunctionDef): MapRecord = new MapRecord(source, expr)

  def unapply(arg: MapRecord): Option[(GraphNodeExpression, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * The definition of how a named field is computed using a function of one or more input streams.
 *
 * @param fieldName The name of the field.
 * @param expr      The function that defines how the field is computed.
 */
class FieldDefinition(val fieldName: String, val expr: FunctionDef) extends Tree {
  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree =
    FieldDefinition(this.fieldName, children.head.asInstanceOf[FunctionDef])

  override def equals(obj: Any): Boolean = obj match {
    case FieldDefinition(n, e) => this.fieldName.equals(n) && this.expr.equals(e)
    case _ => false
  }
}

object FieldDefinition {
  def apply(fieldName: String, expr: FunctionDef): FieldDefinition = new FieldDefinition(fieldName, expr)

  def unapply(arg: FieldDefinition): Option[(String, FunctionDef)] = Some((arg.fieldName, arg.expr))
}


/**
 * An expression representing a map operation that results in a stream of named fields.
 *
 * @param source The input to the map operation.
 * @param fields The output fields.
 */
class MapFields(val source: GraphNodeExpression,
                val fields: List[FieldDefinition],
                val nodeId: String = "",
                val nodeName: String = "") extends MapNodeExpression {

  def this(source: GraphNodeExpression,
           fields: List[FieldDefinition],
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, fields, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new MapFields(this.source, this.fields, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source) ++ fields

  override def replaceChildren(children: List[Tree]): Tree =
    new MapFields(
      children.head.asInstanceOf[GraphNodeExpression],
      children.tail.map(_.asInstanceOf[FieldDefinition]),
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, List(${this.fields.mkString(",")}))"

  override def equals(obj: Any): Boolean = obj match {
    case MapFields(s, f) => this.source.equals(s) && this.fields.equals(f)
    case _ => false
  }
}

object MapFields {
  def apply(source: GraphNodeExpression, fields: List[FieldDefinition]): MapFields =
    new MapFields(source, fields)

  def unapply(arg: MapFields): Option[(GraphNodeExpression, List[FieldDefinition])] = Some((arg.source, arg.fields))
}


trait JoinNodeExpression extends GraphNodeExpression {
  val left: StreamExpression
  val right: StreamExpression
  val joinCondition: FunctionDef
}

object JoinNodeExpression {
  def unapply(arg: JoinNodeExpression): Option[(StreamExpression, StreamExpression, FunctionDef)] =
    Some((arg.left, arg.right, arg.joinCondition))
}


/**
 * An expression representing a left join operation.
 *
 * @param left          The left input stream.
 * @param right         The right input stream.
 * @param joinCondition A function of left and right stream records that defines the join condition.
 */
class LeftJoin(val left: StreamExpression,
               val right: StreamExpression,
               val joinCondition: FunctionDef,
               val nodeId: String = "",
               val nodeName: String = "") extends JoinNodeExpression {

  def this(left: StreamExpression,
           right: StreamExpression,
           joinCondition: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, joinCondition, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new LeftJoin(this.left, this.right, this.joinCondition, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right, this.joinCondition)

  override def replaceChildren(children: List[Tree]): Tree =
    new LeftJoin(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[StreamExpression],
      children(2).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.left}, ${this.right}, ${this.joinCondition})"

  override def equals(obj: Any): Boolean = obj match {
    case LeftJoin(l, r, j) => this.left.equals(l) && this.right.equals(r) && this.joinCondition.equals(j)
    case _ => false
  }
}

object LeftJoin {
  def apply(left: StreamExpression, right: StreamExpression, joinCondition: FunctionDef): LeftJoin =
    new LeftJoin(left, right, joinCondition)

  def unapply(arg: LeftJoin): Option[(StreamExpression, StreamExpression, FunctionDef)] =
    Some((arg.left, arg.right, arg.joinCondition))
}


/**
 * An expression representing a full join operation.
 *
 * @param left          The left input stream.
 * @param right         The right input stream.
 * @param joinCondition A function of left and right stream records that defines the join condition.
 */
class FullJoin(val left: StreamExpression,
               val right: StreamExpression,
               val joinCondition: FunctionDef,
               val nodeId: String = "",
               val nodeName: String = "") extends JoinNodeExpression {

  def this(left: StreamExpression,
           right: StreamExpression,
           joinCondition: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, joinCondition, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new FullJoin(this.left, this.right, this.joinCondition, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right, this.joinCondition)

  override def replaceChildren(children: List[Tree]): Tree =
    new FullJoin(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[StreamExpression],
      children(2).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.left}, ${this.right}, ${this.joinCondition})"

  override def equals(obj: Any): Boolean = obj match {
    case FullJoin(l, r, j) => this.left.equals(l) && this.right.equals(r) && this.joinCondition.equals(j)
    case _ => false
  }
}

object FullJoin {
  def apply(left: StreamExpression, right: StreamExpression, joinCondition: FunctionDef): FullJoin =
    new FullJoin(left, right, joinCondition)

  def unapply(arg: FullJoin): Option[(StreamExpression, StreamExpression, FunctionDef)] =
    Some((arg.left, arg.right, arg.joinCondition))
}


/**
 * Represents a left join between two windowed streams.
 *
 * @param left  The left stream.
 * @param right The right windowed stream.
 */
class WindowedLeftJoin(val left: StreamExpression,
                       val right: WindowExpression,
                       val nodeId: String = "",
                       val nodeName: String = "") extends GraphNodeExpression {
  def this(left: StreamExpression,
           right: WindowExpression,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new WindowedLeftJoin(this.left, this.right, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right)

  override def replaceChildren(children: List[Tree]): Tree =
    new WindowedLeftJoin(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[WindowExpression],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.left}, ${this.right})"

  override def equals(obj: Any): Boolean = obj match {
    case WindowedLeftJoin(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object WindowedLeftJoin {
  def apply(left: StreamExpression, right: WindowExpression): WindowedLeftJoin =
    new WindowedLeftJoin(left, right)

  def unapply(arg: WindowedLeftJoin): Option[(StreamExpression, WindowExpression)] =
    Some((arg.left, arg.right))
}


/**
 * An expression representing a filter operation.
 *
 * @param source    The input to the filter operation.
 * @param predicate The filter predicate which determines which records pass the filter.
 */
class Filter(val source: StreamExpression,
             val predicate: FunctionDef,
             val nodeId: String = "",
             val nodeName: String = "") extends StreamExpression with SingleInputGraphNodeExpression {

  def this(source: StreamExpression,
           predicate: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, predicate, nodeId, nodeName)
    this.tpe = resultType
  }

  override def getInput: GraphNodeExpression = this.source

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new Filter(this.source, this.predicate, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source, predicate)

  override def replaceChildren(children: List[Tree]): Tree =
    new Filter(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.predicate})"

  override def equals(obj: Any): Boolean = obj match {
    case Filter(s, p) => this.source.equals(s) && this.predicate.equals(p)
    case _ => false
  }
}

object Filter {
  def apply(source: StreamExpression, predicate: FunctionDef): Filter = new Filter(source, predicate)

  def unapply(arg: Filter): Option[(StreamExpression, FunctionDef)] = Some((arg.source, arg.predicate))
}


trait GroupingExpression extends GraphNodeExpression with SingleInputGraphNodeExpression {
  val source: GraphNodeExpression
  val expr: FunctionDef

  override def getInput: GraphNodeExpression = this.source

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
  def unapply(arg: GroupingExpression): Option[(GraphNodeExpression, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * An expression representing a group-by operation.
 *
 * @param source The input to the group-by operation.
 * @param expr   A function that defines how group keys are extracted from input records.
 */
class GroupBy(val source: StreamExpression,
              val expr: FunctionDef,
              val nodeId: String = "",
              val nodeName: String = "") extends GroupingExpression {

  def this(source: StreamExpression,
           expr: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, expr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new GroupBy(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new GroupBy(
      children(0).asInstanceOf[StreamExpression],
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
  def apply(source: StreamExpression, expr: FunctionDef): GroupBy = new GroupBy(source, expr)

  def unapply(arg: GroupBy): Option[(StreamExpression, FunctionDef)] = Some((arg.source, arg.expr))
}


trait WindowExpression extends GroupingExpression

object WindowExpression {
  def unapply(arg: WindowExpression): Option[(GraphNodeExpression, FunctionDef)] = Some((arg.source, arg.expr))
}


trait TimeWindowExpression extends WindowExpression {
  val size: Duration
  val offset: Duration
}

object TimeWindowExpression {
  def unapply(arg: TimeWindowExpression): Option[(GraphNodeExpression, FunctionDef, Duration, Duration)] = Some((arg.source, arg.expr, arg.size, arg.offset))
}


/**
 * An expression representing a tumbling window operation.
 *
 * @param source The input to the tumbling window operation.
 * @param period The period of the window.
 * @param offset By default windows are aligned with the epoch, 1970-01-01.
 *               This offset shifts the window alignment to the specified duration after the epoch.
 */
class TumblingWindow(val source: GraphNodeExpression,
                     val dateExtractor: FunctionDef,
                     val period: Duration,
                     val offset: Duration,
                     val nodeId: String = "",
                     val nodeName: String = "") extends TimeWindowExpression {

  val expr: FunctionDef = this.dateExtractor
  val size: Duration = this.period

  def this(source: GraphNodeExpression,
           dateExtractor: FunctionDef,
           period: Duration,
           offset: Duration,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, dateExtractor, period, offset, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new TumblingWindow(this.source, this.dateExtractor, this.period, this.offset, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.dateExtractor, this.period, this.offset)

  override def replaceChildren(children: List[Tree]): Tree =
    new TumblingWindow(
      children(0).asInstanceOf[GraphNodeExpression],
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
  def apply(source: GraphNodeExpression, dateExtractor: FunctionDef, period: Duration, offset: Duration): TumblingWindow =
    new TumblingWindow(source, dateExtractor, period, offset)

  def unapply(arg: TumblingWindow): Option[(GraphNodeExpression, FunctionDef, Duration, Duration)] =
    Some((arg.source, arg.dateExtractor, arg.period, arg.offset))
}


/**
 * An expression representing a tumbling window operation.
 *
 * @param source The input to the tumbling window operation.
 * @param size   The length of a window.
 * @param slide  The distance (in time) between window start times.
 * @param offset By default windows are aligned with the epoch, 1970-01-01.
 *               This offset shifts the window alignment to the specified duration after the epoch.
 */
class SlidingWindow(val source: GraphNodeExpression,
                    val dateExtractor: FunctionDef,
                    val size: Duration,
                    val slide: Duration,
                    val offset: Duration,
                    val nodeId: String = "",
                    val nodeName: String = "") extends TimeWindowExpression {

  val expr: FunctionDef = this.dateExtractor

  def this(source: GraphNodeExpression,
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

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new SlidingWindow(this.source, this.dateExtractor, this.size, this.slide, this.offset, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.dateExtractor, this.size, this.slide, this.offset)

  override def replaceChildren(children: List[Tree]): Tree =
    new SlidingWindow(
      children(0).asInstanceOf[GraphNodeExpression],
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
  def apply(source: GraphNodeExpression, dateExtractor: FunctionDef, size: Duration, slide: Duration, offset: Duration): SlidingWindow =
    new SlidingWindow(source, dateExtractor, size, slide, offset)

  def unapply(arg: SlidingWindow): Option[(GraphNodeExpression, FunctionDef, Duration, Duration, Duration)] =
    Some((arg.source, arg.dateExtractor, arg.size, arg.slide, arg.offset))
}


/**
 * An expression representing an operation that produces a window containing the latest record for every value of a key.
 *
 * @param source        The input to the operation.
 * @param dateExtractor A function that extracts timestamps from input records.
 * @param keyFunc       A function that extracts keys from input records.
 */
class LatestBy(val source: StreamExpression,
               val dateExtractor: FunctionDef,
               val keyFunc: FunctionDef,
               val nodeId: String = "",
               val nodeName: String = "") extends WindowExpression {

  val expr: FunctionDef = keyFunc

  def this(source: StreamExpression,
           dateExtractor: FunctionDef,
           keyFunc: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, dateExtractor, keyFunc, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new LatestBy(this.source, this.dateExtractor, this.keyFunc, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.dateExtractor, this.keyFunc)

  override def replaceChildren(children: List[Tree]): Tree =
    new LatestBy(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[FunctionDef],
      children(2).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.dateExtractor}, ${this.keyFunc})"

  override def equals(obj: Any): Boolean = obj match {
    case LatestBy(s, d, k) => this.source.equals(s) && this.dateExtractor.equals(d) && this.keyFunc.equals(k)
    case _ => false
  }
}

object LatestBy {
  def apply(source: StreamExpression, dateExtractor: FunctionDef, keyFunc: FunctionDef): LatestBy =
    new LatestBy(source, dateExtractor, keyFunc)

  def unapply(arg: LatestBy): Option[(StreamExpression, FunctionDef, FunctionDef)] =
    Some((arg.source, arg.dateExtractor, arg.keyFunc))
}


/**
 * An expression representing a uniqueness constraint on a grouping.
 *
 * @param source The grouping expression on which the uniqueness constraint will be applied.
 * @param expr   A function that defines the key extracted from records that will be unique in the output.
 */
class UniqueBy(val source: GraphNodeExpression,
               val expr: FunctionDef,
               val nodeId: String = "",
               val nodeName: String = "") extends GroupingExpression {

  def this(source: GraphNodeExpression,
           expr: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, expr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): GraphNodeExpression =
    new UniqueBy(this.source, this.expr, id, nodeName, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new UniqueBy(
      children(0).asInstanceOf[GraphNodeExpression],
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.expr})"

  override def equals(obj: Any): Boolean = obj match {
    case UniqueBy(s, e) => this.source.equals(s) && this.expr.equals(e)
    case _ => false
  }
}

object UniqueBy {
  def apply(source: GraphNodeExpression, expr: FunctionDef): UniqueBy = new UniqueBy(source, expr)

  def unapply(arg: UniqueBy): Option[(GraphNodeExpression, FunctionDef)] = Some((arg.source, arg.expr))
}
