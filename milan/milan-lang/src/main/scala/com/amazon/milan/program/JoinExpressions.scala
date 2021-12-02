package com.amazon.milan.program

import com.amazon.milan.Id
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Trait that identifies a stream expression that expresses a join between two streams with a join condition.
 */
trait JoinExpression extends TwoInputStreamExpression {
  override val stateful: Boolean = true

  val condition: FunctionDef

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right, this.condition)

  override def toString: String = s"${this.expressionType}(${this.left}, ${this.right}, ${this.condition})"
}

object JoinExpression {
  def unapply(arg: JoinExpression): Option[(Tree, Tree, FunctionDef)] =
    Some((arg.left, arg.right, arg.condition))
}


/**
 * An expression representing a left join operation.
 *
 * @param left      The left input stream.
 * @param right     The right input stream.
 * @param condition A function that defines the join condition between two records.
 */
@JsonSerialize
@JsonDeserialize
class LeftJoin(val left: Tree,
               val right: Tree,
               val condition: FunctionDef,
               val nodeId: String,
               val nodeName: String) extends JoinExpression {

  def this(left: Tree, right: Tree, condition: FunctionDef, nodeId: String) {
    this(left, right, condition, nodeId, nodeId)
  }

  def this(left: Tree, right: Tree, condition: FunctionDef) {
    this(left, right, condition, Id.newId())
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new LeftJoin(this.left, this.right, this.condition, id, name, this.tpe)

  override def replaceChildren(children: List[Tree]): Tree =
    new LeftJoin(
      children(0),
      children(1),
      children(2).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  def this(left: Tree,
           right: Tree,
           condition: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, condition, nodeId, nodeName)
    this.tpe = resultType
  }

  override def equals(obj: Any): Boolean = obj match {
    case LeftJoin(l, r, c) => this.left.equals(l) && this.right.equals(r) && this.condition.equals(c)
    case _ => false
  }
}

object LeftJoin {
  def apply(left: Tree, right: Tree, condition: FunctionDef): LeftJoin =
    new LeftJoin(left, right, condition)

  def unapply(arg: LeftJoin): Option[(Tree, Tree, FunctionDef)] =
    Some((arg.left, arg.right, arg.condition))
}


/**
 * An expression representing a full join operation.
 *
 * @param left      The left input stream.
 * @param right     The right input stream.
 * @param condition A function that defines the join condition between two records.
 */
@JsonSerialize
@JsonDeserialize
class FullJoin(val left: Tree,
               val right: Tree,
               val condition: FunctionDef,
               val nodeId: String,
               val nodeName: String) extends JoinExpression {

  def this(left: Tree, right: Tree, condition: FunctionDef, nodeId: String) {
    this(left, right, condition, nodeId, nodeId)
  }

  def this(left: Tree, right: Tree, condition: FunctionDef) {
    this(left, right, condition, Id.newId())
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new FullJoin(this.left, this.right, this.condition, id, name, this.tpe)

  def this(left: Tree,
           right: Tree,
           condition: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, condition, nodeId, nodeName)
    this.tpe = resultType
  }

  override def replaceChildren(children: List[Tree]): Tree =
    new FullJoin(
      children(0),
      children(1),
      children(2).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def equals(obj: Any): Boolean = obj match {
    case FullJoin(l, r, c) =>
      this.left.equals(l) && this.right.equals(r) && this.condition.equals(c)

    case _ =>
      false
  }
}

object FullJoin {
  def apply(left: Tree, right: Tree, condition: FunctionDef): FullJoin =
    new FullJoin(left, right, condition)

  def unapply(arg: FullJoin): Option[(Tree, Tree, FunctionDef)] =
    Some((arg.left, arg.right, arg.condition))
}


@JsonSerialize
@JsonDeserialize
class LeftInnerJoin(val left: Tree,
                    val right: Tree,
                    val condition: FunctionDef,
                    val nodeId: String,
                    val nodeName: String) extends JoinExpression {
  def this(left: Tree, right: Tree, condition: FunctionDef, nodeId: String) {
    this(left, right, condition, nodeId, nodeId)
  }

  def this(left: Tree, right: Tree, condition: FunctionDef) {
    this(left, right, condition, Id.newId())
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new LeftInnerJoin(this.left, this.right, this.condition, id, name, this.tpe)

  override def replaceChildren(children: List[Tree]): Tree =
    new LeftInnerJoin(
      children(0),
      children(1),
      children(2).asInstanceOf[FunctionDef],
      this.nodeId,
      this.nodeName,
      this.tpe)

  def this(left: Tree,
           right: Tree,
           condition: FunctionDef,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, condition, nodeId, nodeName)
    this.tpe = resultType
  }

  override def equals(obj: Any): Boolean = obj match {
    case LeftInnerJoin(l, r, c) => this.left.equals(l) && this.right.equals(r) && this.condition.equals(c)
    case _ => false
  }
}

object LeftInnerJoin {
  def apply(left: Tree, right: Tree, condition: FunctionDef): LeftInnerJoin =
    new LeftInnerJoin(left, right, condition)

  def unapply(arg: LeftInnerJoin): Option[(Tree, Tree, FunctionDef)] =
    Some((arg.left, arg.right, arg.condition))
}


/**
 * An expression representing a left join operation.
 *
 * @param left  The left input stream.
 * @param right The right input stream, which must evaluate to a windowed stream.
 */
@JsonSerialize
@JsonDeserialize
class LeftWindowedJoin(val left: Tree,
                       val right: Tree,
                       val nodeId: String,
                       val nodeName: String) extends TwoInputStreamExpression {

  def this(left: Tree, right: Tree, nodeId: String) {
    this(left, right, nodeId, nodeId)
  }

  def this(left: Tree, right: Tree) {
    this(left, right, Id.newId())
  }

  override def withNameAndId(name: String, id: String): StreamExpression =
    new LeftWindowedJoin(this.left, this.right, id, name, this.tpe)

  def this(left: Tree,
           right: Tree,
           nodeId: String,
           nodeName: String,
           resultType: TypeDescriptor[_]) {
    this(left, right, nodeId, nodeName)
    this.tpe = resultType
  }

  override def getChildren: Iterable[Tree] = Seq(left, right)

  override def replaceChildren(children: List[Tree]): Tree =
    new LeftWindowedJoin(
      children(0),
      children(1),
      this.nodeId,
      this.nodeName,
      this.tpe)

  override def equals(obj: Any): Boolean = obj match {
    case LeftWindowedJoin(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object LeftWindowedJoin {
  def apply(left: Tree, right: Tree): LeftWindowedJoin =
    new LeftWindowedJoin(left, right)

  def unapply(arg: LeftWindowedJoin): Option[(Tree, Tree)] =
    Some((arg.left, arg.right))
}
