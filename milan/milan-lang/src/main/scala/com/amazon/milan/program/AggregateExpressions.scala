package com.amazon.milan.program

import com.amazon.milan.typeutil.types
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


trait AggregateExpression extends Tree


trait UnaryAggregateExpression extends AggregateExpression {
  val expr: Tree
}


object UnaryAggregateExpression {
  def unapply(arg: UnaryAggregateExpression): Option[Tree] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class Sum(val expr: Tree) extends UnaryAggregateExpression {
  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree = Sum(children.head)

  override def equals(obj: Any): Boolean = obj match {
    case Sum(e) => this.expr.equals(e)
    case _ => false
  }
}

object Sum {
  def apply(expr: Tree): Sum = new Sum(expr)

  def unapply(arg: Sum): Option[Tree] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class Max(val expr: Tree) extends UnaryAggregateExpression {
  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree = Max(children.head)

  override def equals(obj: Any): Boolean = obj match {
    case Sum(e) => this.expr.equals(e)
    case _ => false
  }
}

object Max {
  def apply(expr: Tree): Max = new Max(expr)

  def unapply(arg: Max): Option[Tree] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class Min(val expr: Tree) extends UnaryAggregateExpression {
  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree = Min(children.head)

  override def equals(obj: Any): Boolean = obj match {
    case Min(e) => this.expr.equals(e)
    case _ => false
  }
}

object Min {
  def apply(expr: Tree): Min = new Min(expr)

  def unapply(arg: Min): Option[Tree] = Some(arg.expr)
}


trait ArgAggregateExpression extends UnaryAggregateExpression {
  val expr: Tuple
}

object ArgAggregateExpression {
  def unapply(arg: ArgAggregateExpression): Option[Tuple] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class ArgMax(val expr: Tuple) extends ArgAggregateExpression {
  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree = ArgMax(children.head.asInstanceOf[Tuple])

  override def equals(obj: Any): Boolean = obj match {
    case ArgMax(e) => this.expr.equals(e)
    case _ => false
  }
}

object ArgMax {
  def apply(expr: Tuple): ArgMax = new ArgMax(expr)

  def unapply(arg: ArgMax): Option[Tuple] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class ArgMin(val expr: Tuple) extends ArgAggregateExpression {
  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree = ArgMin(children.head.asInstanceOf[Tuple])

  override def equals(obj: Any): Boolean = obj match {
    case ArgMin(e) => this.expr.equals(e)
    case _ => false
  }
}

object ArgMin {
  def apply(expr: Tuple): ArgMin = new ArgMin(expr)

  def unapply(arg: ArgMin): Option[Tree] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class Mean(val expr: Tree) extends UnaryAggregateExpression {
  this.tpe = types.Double

  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree = Mean(children.head)

  override def equals(obj: Any): Boolean = obj match {
    case Mean(e) => this.expr.equals(e)
    case _ => false
  }
}

object Mean {
  def apply(expr: Tree): Mean = new Mean(expr)

  def unapply(arg: Mean): Option[Tree] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class First(val expr: Tree) extends UnaryAggregateExpression {
  override def getChildren: Iterable[Tree] = Seq(expr)

  override def replaceChildren(children: List[Tree]): Tree = First(children.head)

  override def equals(obj: Any): Boolean = obj match {
    case First(e) => this.expr.equals(e)
    case _ => false
  }
}

object First {
  def apply(expr: Tree): First = new First(expr)

  def unapply(arg: First): Option[Tree] = Some(arg.expr)
}


@JsonSerialize
@JsonDeserialize
class Count() extends AggregateExpression {
  this.tpe = types.Long

  override def equals(obj: Any): Boolean = obj match {
    case _: Count => true
    case _ => false
  }
}

object Count {
  def apply(): Count = new Count()
}
