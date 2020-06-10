package com.amazon.milan.program

import com.amazon.milan.Id
import com.amazon.milan.typeutil.{StreamTypeDescriptor, TypeDescriptor}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.apache.commons.lang.builder.HashCodeBuilder


/**
 * Trait for expressions that represent data streams or operate on data streams.
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
  def this(id: String, resultType: StreamTypeDescriptor) {
    this(id)
    this.tpe = resultType
  }

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


/**
 * Represents a cycle in the graph.
 *
 * @param source      The upstream input to the cycle.
 * @param cycleNodeId The ID of the downstream node that feeds back into the cycle.
 */
@JsonSerialize
@JsonDeserialize
class Cycle(val source: Tree, var cycleNodeId: String, val nodeId: String, val nodeName: String)
  extends SingleInputStreamExpression {

  def this(source: Tree, cycleNodeId: String, nodeId: String) {
    this(source, cycleNodeId, nodeId, nodeId)
  }

  def this(source: Tree, cycleNodeId: String) {
    this(source, cycleNodeId, Id.newId())
  }

  def this(source: Tree, cycleNodeId: String, nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(source, cycleNodeId, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new Cycle(this.source, this.cycleNodeId, id, name, this.tpe)

  override def replaceChildren(children: List[Tree]): Tree =
    new Cycle(this.source, this.cycleNodeId, this.nodeId, this.nodeName, this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.cycleNodeId})"
}

object Cycle {
  def apply(source: Tree, cycleNodeId: String): Cycle = new Cycle(source, cycleNodeId)

  def unapply(arg: Cycle): Option[(Tree, String)] = Some((arg.source, arg.cycleNodeId))
}


/**
 * Trait that identifies stream expressions that have a single stream as input.
 */
trait SingleInputStreamExpression extends StreamExpression {
  val source: Tree
}

object SingleInputStreamExpression {
  def unapply(arg: SingleInputStreamExpression): Option[Tree] = Some(arg.source)
}


/**
 * Trait that identifies stream expressions that have two streams as input.
 */
trait TwoInputStreamExpression extends StreamExpression {
  val left: Tree
  val right: Tree
}

object TwoInputStreamExpression {
  def unapply(arg: TwoInputStreamExpression): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}


/**
 * An expression representing an aggregation operation on a grouping.
 *
 * @param source The input to the map operation.
 * @param expr   The function that defines the operation.
 */
@JsonSerialize
@JsonDeserialize
class Aggregate(val source: Tree,
                val expr: FunctionDef,
                val nodeId: String,
                val nodeName: String) extends SingleInputStreamExpression {

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
    new Aggregate(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source, expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new Aggregate(
      children(0),
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.expr})"

  override def equals(obj: Any): Boolean = obj match {
    case Aggregate(s, e) => this.source.equals(s) && this.expr.equals(e)
    case _ => false
  }
}

object Aggregate {
  def apply(source: Tree, expr: FunctionDef): Aggregate = new Aggregate(source, expr)

  def unapply(arg: Aggregate): Option[(Tree, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * An expression representing a map operation on a stream.
 *
 * @param source The input to the map operation.
 * @param expr   The function that defines the operation.
 */
@JsonSerialize
@JsonDeserialize
class StreamMap(val source: Tree,
                val expr: FunctionDef,
                val nodeId: String,
                val nodeName: String) extends SingleInputStreamExpression {

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
    new StreamMap(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source, expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new StreamMap(
      children(0),
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.expr})"

  override def equals(obj: Any): Boolean = obj match {
    case StreamMap(s, e) => this.source.equals(s) && this.expr.equals(e)
    case _ => false
  }
}

object StreamMap {
  def apply(source: Tree, expr: FunctionDef): StreamMap = new StreamMap(source, expr)

  def unapply(arg: StreamMap): Option[(Tree, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * An expression representing a flatmap operation that results in a stream of objects.
 *
 * @param source The input to the flatmap operation.
 * @param expr   The function that defines the operation.
 */
@JsonSerialize
@JsonDeserialize
class FlatMap(val source: Tree,
              val expr: FunctionDef,
              val nodeId: String,
              val nodeName: String) extends SingleInputStreamExpression {

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
    new FlatMap(this.source, this.expr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(source, expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new FlatMap(
      children(0),
      children(1).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.expr})"

  override def equals(obj: Any): Boolean = obj match {
    case FlatMap(s, e) => this.source.equals(s) && this.expr.equals(e)
    case _ => false
  }
}

object FlatMap {
  def apply(source: Tree, expr: FunctionDef): FlatMap = new FlatMap(source, expr)

  def unapply(arg: FlatMap): Option[(Tree, FunctionDef)] = Some((arg.source, arg.expr))
}


/**
 * An expression representing a filter operation.
 *
 * @param source    The input to the filter operation.
 * @param predicate The filter predicate which determines which records pass the filter.
 */
@JsonSerialize
@JsonDeserialize
class Filter(val source: Tree,
             val predicate: FunctionDef,
             val nodeId: String,
             val nodeName: String) extends SingleInputStreamExpression {

  def this(source: Tree, predicate: FunctionDef, nodeId: String) {
    this(source, predicate, nodeId, nodeId)
  }

  def this(source: Tree, predicate: FunctionDef) {
    this(source, predicate, Id.newId())
  }

  def this(source: Tree,
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
      children(0),
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
  def apply(source: Tree, predicate: FunctionDef): Filter = new Filter(source, predicate)

  def unapply(arg: Filter): Option[(Tree, FunctionDef)] = Some((arg.source, arg.predicate))
}


/**
 * Base trait for stream expressions that simplify to scan operations.
 */
trait ScanExpression extends SingleInputStreamExpression {
}

object ScanExpression {
  def unapply(arg: ScanExpression): Option[Tree] = Some(arg.source)
}


/**
 * Base trait for scan expressions that extract an argument from records.
 */
trait ArgCompareExpression extends ScanExpression {
  val argExpr: FunctionDef
}

object ArgCompareExpression {
  def unapply(arg: ArgCompareExpression): Option[(Tree, FunctionDef)] = Some((arg.source, arg.argExpr))
}


/**
 * An expression that outputs
 *
 * @param source  The input stream expression.
 * @param argExpr An expression that computes the argument that is used to compare records.
 */
@JsonSerialize
@JsonDeserialize
class StreamArgMax(val source: Tree,
                   val argExpr: FunctionDef,
                   val nodeId: String,
                   val nodeName: String) extends ArgCompareExpression {
  def this(source: Tree, argExpr: FunctionDef, nodeId: String) {
    this(source, argExpr, nodeId, nodeId)
  }

  def this(source: Tree, argExpr: FunctionDef) {
    this(source, argExpr, Id.newId())
  }

  def this(source: Tree, argExpr: FunctionDef, nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(source, argExpr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new StreamArgMax(this.source, this.argExpr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.argExpr)

  override def replaceChildren(children: List[Tree]): Tree =
    new StreamArgMax(children(0), children(1).asInstanceOf[FunctionDef], this.nodeId, this.nodeName, this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.argExpr})"

  override def equals(obj: Any): Boolean = obj match {
    case StreamArgMax(s, e) => this.source.equals(s) && this.argExpr.equals(e)
    case _ => false
  }
}

object StreamArgMax {
  def apply(source: Tree, argExpr: FunctionDef): StreamArgMax = new StreamArgMax(source, argExpr)

  def unapply(arg: StreamArgMax): Option[(Tree, FunctionDef)] = Some((arg.source, arg.argExpr))
}


/**
 * An expression that outputs
 *
 * @param source  The input stream expression.
 * @param argExpr An expression that computes the argument that is used to compare records.
 */
@JsonSerialize
@JsonDeserialize
class StreamArgMin(val source: Tree,
                   val argExpr: FunctionDef,
                   val nodeId: String,
                   val nodeName: String) extends ArgCompareExpression {
  def this(source: Tree, argExpr: FunctionDef, nodeId: String) {
    this(source, argExpr, nodeId, nodeId)
  }

  def this(source: Tree, argExpr: FunctionDef) {
    this(source, argExpr, Id.newId())
  }

  def this(source: Tree, argExpr: FunctionDef, nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(source, argExpr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new StreamArgMin(this.source, this.argExpr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.argExpr)

  override def replaceChildren(children: List[Tree]): Tree =
    new StreamArgMin(children(0), children(1).asInstanceOf[FunctionDef], this.nodeId, this.nodeName, this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.argExpr})"

  override def equals(obj: Any): Boolean = obj match {
    case StreamArgMin(s, e) => this.source.equals(s) && this.argExpr.equals(e)
    case _ => false
  }
}

object StreamArgMin {
  def apply(source: Tree, argExpr: FunctionDef): StreamArgMin = new StreamArgMin(source, argExpr)

  def unapply(arg: StreamArgMin): Option[(Tree, FunctionDef)] = Some((arg.source, arg.argExpr))
}


/**
 * Trait for scan expressions that compute an argument for each input record, and produce an output record
 * based on the argument type and input type.
 */
trait ArgScanExpression extends ScanExpression {
  val argExpr: FunctionDef
  val outputExpr: FunctionDef
}


/**
 * An expression that computes a cumulative sum of values extracted from stream records, and produces
 * an output stream using a function of those values.
 */
@JsonSerialize
@JsonDeserialize
class SumBy(val source: Tree,
            val argExpr: FunctionDef,
            val outputExpr: FunctionDef,
            val nodeId: String,
            val nodeName: String) extends ArgScanExpression {
  def this(source: Tree, argExpr: FunctionDef, outputExpr: FunctionDef, nodeId: String) {
    this(source, argExpr, outputExpr, nodeId, nodeId)
  }

  def this(source: Tree, argExpr: FunctionDef, outputExpr: FunctionDef) {
    this(source, argExpr, outputExpr, Id.newId())
  }

  def this(source: Tree, argExpr: FunctionDef, outputExpr: FunctionDef, nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(source, argExpr, outputExpr, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new SumBy(this.source, this.argExpr, this.outputExpr, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source, this.argExpr, this.outputExpr)

  override def replaceChildren(children: List[Tree]): Tree =
    new SumBy(
      children(0),
      children(1).asInstanceOf[FunctionDef],
      children(2).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source}, ${this.argExpr}, ${this.outputExpr})"

  override def equals(obj: Any): Boolean = obj match {
    case SumBy(s, a, o) => this.source.equals(s) && this.argExpr.equals(a) && this.outputExpr.equals(o)
    case _ => false
  }
}

object SumBy {
  def apply(source: Tree, argExpr: FunctionDef, outputExpr: FunctionDef): SumBy = new SumBy(source, argExpr, outputExpr)

  def unapply(arg: SumBy): Option[(Tree, FunctionDef, FunctionDef)] = Some((arg.source, arg.argExpr, arg.outputExpr))
}


@JsonSerialize
@JsonDeserialize
class Last(val source: Tree,
           val nodeId: String,
           val nodeName: String) extends SingleInputStreamExpression {

  def this(source: Tree, nodeId: String) {
    this(source, nodeId, nodeId)
  }

  def this(source: Tree) {
    this(source, Id.newId())
  }

  def this(source: Tree, nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(source, nodeId, nodeId)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new Last(this.source, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.source)

  override def replaceChildren(children: List[Tree]): Tree =
    new Last(children.head, this.nodeId, this.nodeName, this.tpe)

  override def toString: String = s"${this.expressionType}(${this.source})"

  override def equals(obj: Any): Boolean = obj match {
    case Last(s) => this.source.equals(s)
    case _ => false
  }
}


object Last {
  def apply(source: Tree): Last = new Last(source)

  def unapply(arg: Last): Option[Tree] = Some(arg.source)
}


/**
 * Represents a stream that includes records from two input streams.
 */
class Union(val left: Tree,
            val right: Tree,
            val nodeId: String,
            val nodeName: String)
  extends TwoInputStreamExpression {

  def this(left: Tree, right: Tree, nodeId: String) {
    this(left, right, nodeId, nodeId)
  }

  def this(left: Tree, right: Tree) {
    this(left, right, Id.newId())
  }

  def this(left: Tree, right: Tree, nodeId: String, nodeName: String, resultType: TypeDescriptor[_]) {
    this(left, right, nodeId, nodeName)
    this.tpe = resultType
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new Union(this.left, this.right, id, name, this.tpe)

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right)

  override def replaceChildren(children: List[Tree]): Tree =
    new Union(children(0), children(1), this.nodeId, this.nodeName, this.tpe)

  override def toString: String = s"${this.expressionType}(${this.left}, ${this.right})"

  override def equals(obj: Any): Boolean = obj match {
    case Union(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object Union {
  def apply(left: Tree, right: Tree): Union = new Union(left, right)

  def unapply(arg: Union): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}
