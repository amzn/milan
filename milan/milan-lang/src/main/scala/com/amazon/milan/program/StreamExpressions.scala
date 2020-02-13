package com.amazon.milan.program

import com.amazon.milan.Id
import com.amazon.milan.typeutil.{StreamTypeDescriptor, TypeDescriptor}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.commons.lang.builder.HashCodeBuilder


/**
 * Trait for graph expressions that represent data streams.
 */
@JsonSerialize(using = classOf[TreeSerializer])
@JsonDeserialize(using = classOf[TreeDeserializer])
trait StreamExpression extends Tree {
  val nodeId: String
  val nodeName: String

  def withName(name: String): StreamExpression = this.withNameAndId(name, this.nodeId)

  def withId(id: String): StreamExpression = {
    // Setting the ID when the name hasn't been set, sets the name to the new ID.
    // Otherwise the name looks like and ID but is different from the ID, which is weird.
    if (this.nodeId == this.nodeName) {
      this.withNameAndId(id, id)
    }
    else {
      this.withNameAndId(this.nodeName, id)
    }
  }

  def withNameAndId(name: String, id: String): StreamExpression

  override def hashCode(): Int = HashCodeBuilder.reflectionHashCode(this)

  @JsonIgnore
  def streamType: StreamTypeDescriptor = this.tpe.asStream

  @JsonIgnore
  def recordType: TypeDescriptor[_] = this.streamType.recordType
}


/**
 * An expression that marks a location in an expression tree so that it can be located later.
 *
 * @param id The unique ID of the marker.
 */
@JsonSerialize
@JsonDeserialize
class Marker(val id: String) extends StreamExpression {
  override val nodeId: String = this.id

  override val nodeName: String = this.id

  override def withNameAndId(name: String, id: String): StreamExpression = throw new NotImplementedError()

  override def equals(obj: Any): Boolean = obj match {
    case Marker(i) => id == i
    case _ => false
  }
}

object Marker {
  def apply(id: String): Marker = new Marker(id)

  def unapply(arg: Marker): Option[String] = Some(arg.id)
}


@JsonSerialize
@JsonDeserialize
class ExternalStream(val nodeId: String, val nodeName: String, streamType: StreamTypeDescriptor) extends StreamExpression {
  this.tpe = streamType

  override def withNameAndId(name: String, id: String): StreamExpression = new ExternalStream(id, name, this.streamType)

  override def toString: String = s"""${this.expressionType}("${this.nodeId}", "${this.nodeName}")"""

  override def equals(obj: Any): Boolean = obj match {
    case o: ExternalStream => o.nodeId == this.nodeId && o.nodeName == this.nodeName && o.tpe == this.tpe
    case _ => false
  }
}

object ExternalStream {
  def apply(nodeId: String, nodeName: String, streamType: StreamTypeDescriptor): ExternalStream = new ExternalStream(nodeId, nodeName, streamType)

  def unapply(arg: ExternalStream): Option[(String, String, StreamTypeDescriptor)] = Some((arg.nodeId, arg.nodeName, arg.tpe.asStream))
}


/**
 * A reference to a graph node.
 *
 * @param nodeId   The ID of the referenced node.
 * @param nodeName The name of the referenced node.
 */
@JsonSerialize
@JsonDeserialize
class Ref(val nodeId: String, val nodeName: String) extends StreamExpression {
  def this(nodeId: String) {
    this(nodeId, nodeId)
  }

  def this(nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression = new Ref(id, name, this.tpe)

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


trait MapExpression extends StreamExpression {
  val source: Tree
}

object MapExpression {
  def unapply(arg: MapExpression): Option[Tree] = Some(arg.source)
}


/**
 * An expression representing a map operation that results in a stream of objects.
 *
 * @param source The input to the map operation.
 * @param expr   The function that defines the operation.
 */
@JsonSerialize
@JsonDeserialize
class MapRecord(val source: Tree,
                val expr: FunctionDef,
                val nodeId: String,
                val nodeName: String) extends MapExpression {

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
    new MapRecord(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source, expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new MapRecord(
      children(0),
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
  def apply(source: Tree, expr: FunctionDef): MapRecord = new MapRecord(source, expr)

  def unapply(arg: MapRecord): Option[(Tree, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * The definition of how a named field is computed using a function of one or more input streams.
 *
 * @param fieldName The name of the field.
 * @param expr      The function that defines how the field is computed.
 */
@JsonSerialize
@JsonDeserialize
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
@JsonSerialize
@JsonDeserialize
class MapFields(val source: StreamExpression,
                val fields: List[FieldDefinition],
                val nodeId: String,
                val nodeName: String) extends MapExpression {

  def this(source: StreamExpression, fields: List[FieldDefinition], nodeId: String) {
    this(source, fields, nodeId, nodeId)
  }

  def this(source: StreamExpression, fields: List[FieldDefinition]) {
    this(source, fields, Id.newId())
  }

  def this(source: StreamExpression,
           fields: List[FieldDefinition],
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, fields, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new MapFields(this.source, this.fields, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source) ++ fields

  override def replaceChildren(children: List[Tree]): Tree =
    new MapFields(
      children.head.asInstanceOf[StreamExpression],
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
  def apply(source: StreamExpression, fields: List[FieldDefinition]): MapFields =
    new MapFields(source, fields)

  def unapply(arg: MapFields): Option[(StreamExpression, List[FieldDefinition])] = Some((arg.source, arg.fields))
}


/**
 * Base trait for FlatMap expressions.
 */
trait FlatMapExpression extends StreamExpression {
  val source: StreamExpression
}

object FlatMapExpression {
  def unapply(arg: FlatMapExpression): Option[Tree] = Some(arg.source)
}


/**
 * An expression representing a flatmap operation that results in a stream of objects.
 *
 * @param source The input to the map operation.
 * @param expr   The function that defines the operation.
 */
@JsonSerialize
@JsonDeserialize
class FlatMap(val source: StreamExpression,
              val expr: FunctionDef,
              val nodeId: String,
              val nodeName: String) extends FlatMapExpression {

  def this(source: StreamExpression, expr: FunctionDef, nodeId: String) {
    this(source, expr, nodeId, nodeId)
  }

  def this(source: StreamExpression, expr: FunctionDef) {
    this(source, expr, Id.newId())
  }

  def this(source: StreamExpression,
           expr: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, expr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new FlatMap(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = List(source, expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new FlatMap(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def equals(obj: Any): Boolean = obj match {
    case FlatMap(s, e) => this.source.equals(s) && this.expr.equals(e)
    case _ => false
  }
}

object FlatMap {
  def apply(source: StreamExpression, expr: FunctionDef): FlatMap = new FlatMap(source, expr)

  def unapply(arg: FlatMap): Option[(StreamExpression, FunctionDef)] = Some((arg.source, arg.expr))
}


trait JoinExpression extends StreamExpression {
  val left: StreamExpression
  val right: StreamExpression
}

object JoinExpression {
  def unapply(arg: JoinExpression): Option[(StreamExpression, StreamExpression)] =
    Some((arg.left, arg.right))
}


/**
 * An expression representing a left join operation.
 *
 * @param left  The left input stream.
 * @param right The right input stream.
 */
@JsonSerialize
@JsonDeserialize
class LeftJoin(val left: StreamExpression,
               val right: StreamExpression,
               val nodeId: String,
               val nodeName: String) extends JoinExpression {

  def this(left: StreamExpression, right: StreamExpression, nodeId: String) {
    this(left, right, nodeId, nodeId)
  }

  def this(left: StreamExpression, right: StreamExpression) {
    this(left, right, Id.newId())
  }

  def this(left: StreamExpression,
           right: StreamExpression,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new LeftJoin(this.left, this.right, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right)

  override def replaceChildren(children: List[Tree]): Tree =
    new LeftJoin(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[StreamExpression],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.left}, ${this.right})"

  override def equals(obj: Any): Boolean = obj match {
    case LeftJoin(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object LeftJoin {
  def apply(left: StreamExpression, right: StreamExpression): LeftJoin =
    new LeftJoin(left, right)

  def unapply(arg: LeftJoin): Option[(StreamExpression, StreamExpression)] =
    Some((arg.left, arg.right))
}


/**
 * An expression representing a full join operation.
 *
 * @param left  The left input stream.
 * @param right The right input stream.
 */
@JsonSerialize
@JsonDeserialize
class FullJoin(val left: StreamExpression,
               val right: StreamExpression,
               val nodeId: String,
               val nodeName: String) extends JoinExpression {

  def this(left: StreamExpression, right: StreamExpression, nodeId: String) {
    this(left, right, nodeId, nodeId)
  }

  def this(left: StreamExpression, right: StreamExpression) {
    this(left, right, Id.newId())
  }

  def this(left: StreamExpression,
           right: StreamExpression,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new FullJoin(this.left, this.right, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right)

  override def replaceChildren(children: List[Tree]): Tree =
    new FullJoin(
      children(0).asInstanceOf[StreamExpression],
      children(1).asInstanceOf[StreamExpression],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.left}, ${this.right})"

  override def equals(obj: Any): Boolean = obj match {
    case FullJoin(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object FullJoin {
  def apply(left: StreamExpression, right: StreamExpression): FullJoin =
    new FullJoin(left, right)

  def unapply(arg: FullJoin): Option[(StreamExpression, StreamExpression)] =
    Some((arg.left, arg.right))
}


/**
 * An expression representing a filter operation.
 *
 * @param source    The input to the filter operation.
 * @param predicate The filter predicate which determines which records pass the filter.
 */
@JsonSerialize
@JsonDeserialize
class Filter(val source: StreamExpression,
             val predicate: FunctionDef,
             val nodeId: String,
             val nodeName: String) extends StreamExpression {

  def this(source: StreamExpression, predicate: FunctionDef, nodeId: String) {
    this(source, predicate, nodeId, nodeId)
  }

  def this(source: StreamExpression, predicate: FunctionDef) {
    this(source, predicate, Id.newId())
  }

  def this(source: StreamExpression,
           predicate: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, predicate, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
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


trait GroupingExpression extends StreamExpression {
  val source: StreamExpression
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
  def unapply(arg: GroupingExpression): Option[(StreamExpression, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * An expression representing a group-by operation.
 *
 * @param source The input to the group-by operation.
 * @param expr   A function that defines how group keys are extracted from input records.
 */
@JsonSerialize
@JsonDeserialize
class GroupBy(val source: StreamExpression,
              val expr: FunctionDef,
              val nodeId: String,
              val nodeName: String) extends GroupingExpression {

  def this(source: StreamExpression, expr: FunctionDef, nodeId: String) {
    this(source, expr, nodeId, nodeId)
  }

  def this(source: StreamExpression, expr: FunctionDef) {
    this(source, expr, Id.newId())
  }

  def this(source: StreamExpression,
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
  def unapply(arg: WindowExpression): Option[(StreamExpression, FunctionDef)] = Some((arg.source, arg.expr))
}


trait TimeWindowExpression extends WindowExpression {
  val size: Duration
  val offset: Duration
}

object TimeWindowExpression {
  def unapply(arg: TimeWindowExpression): Option[(StreamExpression, FunctionDef, Duration, Duration)] = Some((arg.source, arg.expr, arg.size, arg.offset))
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
class TumblingWindow(val source: StreamExpression,
                     val dateExtractor: FunctionDef,
                     val period: Duration,
                     val offset: Duration,
                     val nodeId: String,
                     val nodeName: String) extends TimeWindowExpression {

  val expr: FunctionDef = this.dateExtractor
  val size: Duration = this.period

  def this(source: StreamExpression, dateExtractor: FunctionDef, period: Duration, offset: Duration, nodeId: String) {
    this(source, dateExtractor, period, offset, nodeId, nodeId)
  }

  def this(source: StreamExpression, dateExtractor: FunctionDef, period: Duration, offset: Duration) {
    this(source, dateExtractor, period, offset, Id.newId())
  }

  def this(source: StreamExpression,
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
      children(0).asInstanceOf[StreamExpression],
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
  def apply(source: StreamExpression, dateExtractor: FunctionDef, period: Duration, offset: Duration): TumblingWindow =
    new TumblingWindow(source, dateExtractor, period, offset)

  def unapply(arg: TumblingWindow): Option[(StreamExpression, FunctionDef, Duration, Duration)] =
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
@JsonSerialize
@JsonDeserialize
class SlidingWindow(val source: StreamExpression,
                    val dateExtractor: FunctionDef,
                    val size: Duration,
                    val slide: Duration,
                    val offset: Duration,
                    val nodeId: String,
                    val nodeName: String) extends TimeWindowExpression {

  val expr: FunctionDef = this.dateExtractor

  def this(source: StreamExpression, dateExtractor: FunctionDef, size: Duration, slide: Duration, offset: Duration, nodeId: String) {
    this(source, dateExtractor, size, slide, offset, nodeId, nodeId)
  }

  def this(source: StreamExpression, dateExtractor: FunctionDef, size: Duration, slide: Duration, offset: Duration) {
    this(source, dateExtractor, size, slide, offset, Id.newId())
  }

  def this(source: StreamExpression,
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
      children(0).asInstanceOf[StreamExpression],
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
  def apply(source: StreamExpression, dateExtractor: FunctionDef, size: Duration, slide: Duration, offset: Duration): SlidingWindow =
    new SlidingWindow(source, dateExtractor, size, slide, offset)

  def unapply(arg: SlidingWindow): Option[(StreamExpression, FunctionDef, Duration, Duration, Duration)] =
    Some((arg.source, arg.dateExtractor, arg.size, arg.slide, arg.offset))
}


/**
 * An expression representing an operation that produces a window containing the latest record for every value of a key.
 *
 * @param source        The input to the operation.
 * @param dateExtractor A function that extracts timestamps from input records.
 * @param keyFunc       A function that extracts keys from input records.
 */
@JsonSerialize
@JsonDeserialize
class LatestBy(val source: StreamExpression,
               val dateExtractor: FunctionDef,
               val keyFunc: FunctionDef,
               val nodeId: String,
               val nodeName: String) extends WindowExpression {

  val expr: FunctionDef = keyFunc

  def this(source: StreamExpression, dateExtractor: FunctionDef, keyFunc: FunctionDef, nodeId: String) {
    this(source, dateExtractor, keyFunc, nodeId, nodeId)
  }

  def this(source: StreamExpression, dateExtractor: FunctionDef, keyFunc: FunctionDef) {
    this(source, dateExtractor, keyFunc, Id.newId())
  }

  def this(source: StreamExpression,
           dateExtractor: FunctionDef,
           keyFunc: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, dateExtractor, keyFunc, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
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
@JsonSerialize
@JsonDeserialize
class UniqueBy(val source: StreamExpression,
               val expr: FunctionDef,
               val nodeId: String,
               val nodeName: String) extends GroupingExpression {

  def this(source: StreamExpression, expr: FunctionDef, nodeId: String) {
    this(source, expr, nodeId, nodeId)
  }

  def this(source: StreamExpression, expr: FunctionDef) {
    this(source, expr, Id.newId())
  }

  def this(source: StreamExpression,
           expr: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(source, expr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new UniqueBy(this.source, this.expr, id, nodeName, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new UniqueBy(
      children(0).asInstanceOf[StreamExpression],
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
  def apply(source: StreamExpression, expr: FunctionDef): UniqueBy = new UniqueBy(source, expr)

  def unapply(arg: UniqueBy): Option[(StreamExpression, FunctionDef)] = Some((arg.source, arg.expr))
}
