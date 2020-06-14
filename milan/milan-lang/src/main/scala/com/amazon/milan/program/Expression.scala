package com.amazon.milan.program

import com.amazon.milan.typeutil.{TypeDescriptor, types}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

/*
This file contains the expression types for Milan expression trees.

Expression tree classes follow these rules:
1. The default constructor must be the only constructor, otherwise it must override toString and not rely on Tree.toString
2. It must have a companion object with an unapply method, because it must be usable in case statements.
3. It must provide an equals() override.
4. If the return type does not depend on the types of its children, it should set its tpe property in the constructor.
   See And for an example of this.
5. If it takes its return type in the constructor, that argument should be private, and its tpe property set to that
   value in the constructor body. See ConstantValue for an example of this.
6. If it has any child nodes, override the replaceChildren method.

In order for a new expression type to be used, it must be added here and support added for it in several other places:
1. com.amazon.milan.program.internal.ConvertExpressionHost.getMilanExpressionTree - this is where scala expressions are parsed
   into Milan expressions.
2. com.amazon.milan.program.internal.LiftableImpls
3. com.amazon.milan.program.TreeParser.convertNode
4. com.amazon.milan.compiler.scala.ScalarFunctionGenerator.ConversionContext.getScalaCode
5. com.amazon.milan.program.TypeChecker.typeCheckFunctionBody, if the return type of the expression depends on its
   children
 */


/**
 * A expression representing a constant value.
 *
 * @param value The constant value.
 */
@JsonSerialize
@JsonDeserialize
class ConstantValue(val value: Any, private val valueType: TypeDescriptor[_]) extends Tree {
  this.tpe = valueType

  override def equals(obj: Any): Boolean = obj match {
    case ConstantValue(v, t) => this.value.equals(v) && this.valueType.equals(t)
    case _ => false
  }
}

object ConstantValue {
  def apply(value: Any, valueType: TypeDescriptor[_]): ConstantValue = new ConstantValue(value, valueType)

  def unapply(arg: ConstantValue): Option[(Any, TypeDescriptor[_])] = Some((arg.value, arg.tpe))
}


/**
 * An expression representing a period of time.
 *
 * @param milliseconds The length of the duration in milliseconds.
 */
@JsonSerialize
@JsonDeserialize
class Duration(val milliseconds: Long) extends Tree {
  this.tpe = types.Duration

  override def toString: String = s"Duration(${this.milliseconds})"

  override def equals(obj: Any): Boolean = obj match {
    case Duration(ms) => this.milliseconds.equals(ms)
    case _ => false
  }
}

object Duration {
  val ZERO: Duration = new Duration(0)

  def apply(milliseconds: Long): Duration = new Duration(milliseconds)

  def unapply(arg: Duration): Option[Long] = Some(arg.milliseconds)
}


/**
 * An expression that represents a named field.
 */
@JsonSerialize
@JsonDeserialize
class NamedField(val fieldName: String, val expr: Tree) extends Tree {
  override def getChildren: Iterable[Tree] = Seq(this.expr)

  override def replaceChildren(children: List[Tree]): Tree =
    new NamedField(this.fieldName, children.head)

  override def equals(obj: Any): Boolean = obj match {
    case NamedField(n, e) => this.fieldName.equals(n) && this.expr.equals(e)
    case _ => false
  }
}

object NamedField {
  def apply(fieldName: String, expr: Tree): NamedField = new NamedField(fieldName, expr)

  def unapply(arg: NamedField): Option[(String, Tree)] = Some((arg.fieldName, arg.expr))
}


/**
 * An expression that constructs a tuple where the elements have associated names.
 */
@JsonSerialize
@JsonDeserialize
class NamedFields(val fields: List[NamedField]) extends Tree {
  override def getChildren: Iterable[Tree] = this.fields

  override def replaceChildren(children: List[Tree]): Tree =
    new NamedFields(children.map(_.asInstanceOf[NamedField]))

  override def equals(obj: Any): Boolean = obj match {
    case NamedFields(f) => this.fields.equals(f)
    case _ => false
  }
}

object NamedFields {
  def apply(fields: List[NamedField]): NamedFields = new NamedFields(fields)

  def unapply(arg: NamedFields): Option[List[NamedField]] = Some(arg.fields)
}


/**
 * An expression representing an if-else statement.
 *
 * @param condition A condition expression.
 * @param thenExpr  The expression to evaluate when the condition is true.
 * @param elseExpr  The expression to evaluate when the condition is false.
 */
@JsonSerialize
@JsonDeserialize
class IfThenElse(val condition: Tree, val thenExpr: Tree, val elseExpr: Tree) extends Tree {
  override def getChildren: Iterable[Tree] = List(condition, thenExpr, elseExpr)

  override def replaceChildren(children: List[Tree]): Tree = IfThenElse(children(0), children(1), children(2))

  override def equals(obj: Any): Boolean = obj match {
    case IfThenElse(c, i, e) => this.condition.equals(c) && this.thenExpr.equals(i) && this.elseExpr.equals(e)
    case _ => false
  }
}

object IfThenElse {
  def apply(condition: Tree, thenExpr: Tree, elseExpr: Tree): IfThenElse = new IfThenElse(condition, thenExpr, elseExpr)

  def unapply(arg: IfThenElse): Option[(Tree, Tree, Tree)] = Some((arg.condition, arg.thenExpr, arg.elseExpr))
}


/**
 * An expression representing a null check.
 *
 * @param expr The expression to check for null.
 */
@JsonSerialize
@JsonDeserialize
class IsNull(val expr: Tree) extends Tree {
  this.tpe = types.Boolean

  override def getChildren: Iterable[Tree] = List(expr)

  override def replaceChildren(children: List[Tree]): Tree = IsNull(children.head)

  override def equals(obj: Any): Boolean = obj match {
    case IsNull(e) => this.expr.equals(e)
    case _ => false
  }
}

object IsNull {
  def apply(expr: Tree): IsNull = new IsNull(expr)

  def unapply(arg: IsNull): Option[Tree] = Some(arg.expr)
}


/**
 * An expression representing a boolean NOT.
 *
 * @param expr The input expression.
 */
@JsonSerialize
@JsonDeserialize
class Not(val expr: Tree) extends Tree {
  this.tpe = types.Boolean

  override def getChildren: Iterable[Tree] = List(expr)

  override def replaceChildren(children: List[Tree]): Tree = Not(children.head)

  override def equals(obj: Any): Boolean = obj match {
    case Not(e) => this.expr.equals(e)
    case _ => false
  }
}

object Not {
  def apply(expr: Tree): Not = new Not(expr)

  def unapply(arg: Not): Option[Tree] = Some(arg.expr)
}


@JsonSerialize(using = classOf[TreeSerializer])
@JsonDeserialize(using = classOf[TreeDeserializer])
trait SelectExpression extends Tree


/**
 * An expression referencing a field of a value by name.
 *
 * @param qualifier The qualifier of the field.
 * @param fieldName The name of the field.
 */
@JsonSerialize
@JsonDeserialize
class SelectField(val qualifier: Tree, val fieldName: String) extends SelectExpression {
  override def getChildren: Iterable[Tree] = Seq(this.qualifier)

  override def replaceChildren(children: List[Tree]): Tree =
    SelectField(children.head, this.fieldName)

  override def equals(obj: Any): Boolean = obj match {
    case SelectField(a, f) => this.qualifier.equals(a) && this.fieldName.equals(f)
    case _ => false
  }
}

object SelectField {
  def apply(qualifier: Tree, fieldName: String): SelectField = new SelectField(qualifier, fieldName)

  def unapply(arg: SelectField): Option[(Tree, String)] = Some((arg.qualifier, arg.fieldName))
}


/**
 * An expression referencing a value by name.
 *
 * @param termName The name of the function argument.
 */
@JsonSerialize
@JsonDeserialize
class SelectTerm(val termName: String) extends SelectExpression {
  override def equals(obj: Any): Boolean = obj match {
    case SelectTerm(a) => this.termName.equals(a)
    case _ => false
  }
}

object SelectTerm {
  def apply(argumentName: String): SelectTerm = new SelectTerm(argumentName)

  def unapply(arg: SelectTerm): Option[String] = Some(arg.termName)
}


/**
 * An expression that unpacks the fields of a tuple expression into named variables.
 *
 * @param names The names to assign to the fields of the tuple in the inner expression.
 */
@JsonSerialize
@JsonDeserialize
class Unpack(val target: Tree, val names: List[String], val body: Tree) extends Tree {
  override def getChildren: Iterable[Tree] = List(target, body)

  override def replaceChildren(children: List[Tree]): Tree =
    Unpack(children(0), this.names, children(1))

  override def equals(obj: Any): Boolean = obj match {
    case Unpack(v, n, b) => this.target.equals(v) && this.names.equals(n) && this.body.equals(b)
    case _ => false
  }
}

object Unpack {
  def apply(target: Tree, valueNames: List[String], body: Tree): Unpack = new Unpack(target, valueNames, body)

  def unapply(arg: Unpack): Option[(Tree, List[String], Tree)] = Some(arg.target, arg.names, arg.body)
}


/**
 * Gets a single element from a tuple.
 *
 * @param target An expression that returns a tuple.
 * @param index  An expression that returns the zero-based index of the element to get.
 */
class TupleElement(val target: Tree, val index: Int) extends Tree {
  override def getChildren: Iterable[Tree] = Seq(this.target)

  override def replaceChildren(children: List[Tree]): Tree =
    TupleElement(children.head, this.index)

  override def equals(obj: Any): Boolean = obj match {
    case TupleElement(t, i) => this.target.equals(t) && this.index.equals(i)
    case _ => false
  }
}

object TupleElement {
  def apply(target: Tree, index: Int): TupleElement = new TupleElement(target, index)

  def unapply(arg: TupleElement): Option[(Tree, Int)] = Some((arg.target, arg.index))
}


/**
 * An expression representing a named value.
 *
 * @param name The name of the value.
 */
@JsonSerialize
@JsonDeserialize
class ValueDef(val name: String) extends Tree {
  def this(name: String, ty: TypeDescriptor[_]) {
    this(name)
    this.tpe = ty
  }

  override def replaceChildren(children: List[Tree]): Tree = new ValueDef(this.name, this.tpe)

  override def equals(obj: Any): Boolean = obj match {
    case ValueDef(n, t) => this.name == n && (this.tpe == null || t == null || this.tpe.equals(t))
    case _ => false
  }
}

object ValueDef {
  def apply(name: String, ty: TypeDescriptor[_]): ValueDef = new ValueDef(name, ty)

  def named(name: String): ValueDef = new ValueDef(name)

  def unapply(arg: ValueDef): Option[(String, TypeDescriptor[_])] = Some((arg.name, arg.tpe))
}


/**
 * An expression representing a function definition.
 *
 * @param arguments The names of the arguments of the function.
 * @param body      The function body.
 */
@JsonSerialize
@JsonDeserialize
class FunctionDef(val arguments: List[ValueDef], val body: Tree) extends Tree {
  def withArgumentTypes(argumentTypes: List[TypeDescriptor[_]]): FunctionDef = {
    if (argumentTypes.length != arguments.length) {
      throw new IllegalArgumentException("Argument type list must be the same size as the function arguments.")
    }

    val newArguments = arguments.zip(argumentTypes).map { case (arg, argType) => ValueDef(arg.name, argType) }
    val newFunctionDef = new FunctionDef(newArguments, this.body)
    newFunctionDef.tpe = this.tpe

    newFunctionDef
  }

  /**
   * Gets a copy of this [[FunctionDef]] with the return type changed.
   */
  def withReturnType(returnType: TypeDescriptor[_]): FunctionDef = {
    val copy = FunctionDef(this.arguments, this.body)
    copy.tpe = returnType
    copy
  }

  override def getChildren: Iterable[Tree] = arguments ++ List(body)

  override def replaceChildren(children: List[Tree]): Tree =
    FunctionDef(this.arguments.take(this.arguments.length), children.last)

  override def equals(obj: Any): Boolean = obj match {
    case FunctionDef(a, e) => this.arguments.equals(a) && this.body.equals(e)
    case _ => false
  }
}

object FunctionDef {
  def create(argumentNames: List[String], expr: Tree): FunctionDef = {
    new FunctionDef(argumentNames.map(ValueDef.named), expr)
  }

  def apply(arguments: List[ValueDef], expr: Tree): FunctionDef = new FunctionDef(arguments, expr)

  def unapply(arg: FunctionDef): Option[(List[ValueDef], Tree)] = Some((arg.arguments, arg.body))
}


/**
 * An expression representing a call to a function not contained in the tree.
 * The function may be static or on an object instance.
 *
 * @param function A reference to the function.
 * @param args     The arguments that are passed to the function.
 */
@JsonSerialize
@JsonDeserialize
class ApplyFunction(val function: FunctionReference, val args: List[Tree], private val returnType: TypeDescriptor[_]) extends Tree {
  this.tpe = returnType

  override def getChildren: Iterable[Tree] = this.args

  override def replaceChildren(children: List[Tree]): Tree = ApplyFunction(this.function, children, this.tpe)

  override def equals(obj: Any): Boolean = obj match {
    case ApplyFunction(f, a, r) => this.function.equals(f) && this.args.equals(a) && this.tpe == r
    case _ => false
  }
}

object ApplyFunction {
  def apply(function: FunctionReference, args: List[Tree], returnType: TypeDescriptor[_]) =
    new ApplyFunction(function, args, returnType)

  def unapply(arg: ApplyFunction): Option[(FunctionReference, List[Tree], TypeDescriptor[_])] =
    Some((arg.function, arg.args, arg.tpe))
}


/**
 * An expression representing a type conversion.
 *
 * @param expr       The expression to convert.
 * @param targetType The type to convert to.
 */
@JsonSerialize
@JsonDeserialize
class ConvertType(val expr: Tree, targetType: TypeDescriptor[_]) extends Tree {
  this.tpe = targetType

  override def getChildren: Iterable[Tree] = List(this.expr)

  override def replaceChildren(children: List[Tree]): Tree = ConvertType(children.head, this.tpe)

  override def equals(obj: Any): Boolean = obj match {
    case ConvertType(e, t) => this.expr.equals(e) && this.tpe.equals(t)
  }
}

object ConvertType {
  def apply(expr: Tree, targetType: TypeDescriptor[_]): ConvertType = new ConvertType(expr, targetType)

  def unapply(arg: ConvertType): Option[(Tree, TypeDescriptor[_])] = Some((arg.expr, arg.tpe))
}


/**
 * An expression representing creating an instance of a type.
 *
 * @param ty   The type being instantiated.
 * @param args The constructor arguments.
 */
@JsonSerialize
@JsonDeserialize
class CreateInstance(val ty: TypeDescriptor[_], val args: List[Tree]) extends Tree {
  this.tpe = ty

  override def getChildren: Iterable[Tree] = this.args

  override def replaceChildren(children: List[Tree]): Tree = CreateInstance(this.ty, children)

  override def equals(obj: Any): Boolean = obj match {
    case CreateInstance(t, a) => this.ty.equals(t) && this.args.equals(a)
  }
}

object CreateInstance {
  def apply(ty: TypeDescriptor[_], args: List[Tree]): CreateInstance = new CreateInstance(ty, args)

  def unapply(arg: CreateInstance): Option[(TypeDescriptor[_], List[Tree])] = Some((arg.ty, arg.args))
}


/**
 * An expression representing making a list out of the results of child trees.
 *
 * @param elements The trees whose output is put in the list.
 */
@JsonSerialize
@JsonDeserialize
class Tuple(val elements: List[Tree]) extends Tree {
  override def getChildren: Iterable[Tree] = this.elements

  override def replaceChildren(children: List[Tree]): Tree = Tuple(children)

  override def equals(obj: Any): Boolean = obj match {
    case Tuple(e) => this.elements.equals(e)
  }
}

object Tuple {
  def apply(elements: List[Tree]): Tuple = new Tuple(elements)

  def unapply(arg: Tuple): Option[List[Tree]] = Some(arg.elements)
}


/**
 * Base class for binary mathematical operators.
 */
@JsonSerialize
@JsonDeserialize
abstract class BinaryMathOperator(val isAssociative: Boolean) extends Tree {
  val left: Tree
  val right: Tree

  override def getChildren: Iterable[Tree] = List(this.left, this.right)

  override def equals(obj: Any): Boolean = obj match {
    case b: BinaryMathOperator => this.expressionType == b.expressionType && this.left.equals(b.left) && this.right.equals(b.right)
  }
}

object BinaryMathOperator {
  def unapply(arg: BinaryMathOperator): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}


class Plus(val left: Tree, val right: Tree) extends BinaryMathOperator(true) {
  override def replaceChildren(children: List[Tree]): Tree = Plus(children(0), children(1))
}

object Plus {
  def apply(left: Tree, right: Tree): Plus = new Plus(left, right)

  def unapply(arg: Plus): Option[(Tree, Tree)] = BinaryMathOperator.unapply(arg)
}


class Minus(val left: Tree, val right: Tree) extends BinaryMathOperator(true) {
  override def replaceChildren(children: List[Tree]): Tree = Minus(children(0), children(1))
}

object Minus {
  def apply(left: Tree, right: Tree): Minus = new Minus(left, right)

  def unapply(arg: Minus): Option[(Tree, Tree)] = BinaryMathOperator.unapply(arg)
}


/**
 * Base class for binary logical operators.
 */
@JsonSerialize
@JsonDeserialize
abstract class BinaryLogicalOperator extends Tree {
  val left: Tree
  val right: Tree

  this.tpe = types.Boolean

  override def getChildren: Iterable[Tree] = Seq(this.left, this.right)
}

object BinaryLogicalOperator {
  def unapply(arg: BinaryLogicalOperator): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}


/**
 * An expression representing a boolean AND of two boolean expressions.
 *
 * @param left  The first expression.
 * @param right The second expression.
 */
class And(val left: Tree, val right: Tree) extends BinaryLogicalOperator {
  override def replaceChildren(children: List[Tree]): Tree = And(children(0), children(1))

  override def equals(obj: Any): Boolean = obj match {
    case And(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object And {
  def apply(left: Tree, right: Tree): And = new And(left, right)

  def unapply(arg: And): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}


/**
 * An expression representing an equality test between two expressions.
 *
 * @param left  The first expression.
 * @param right The second expression.
 */
class Equals(val left: Tree, val right: Tree) extends BinaryLogicalOperator {
  override def replaceChildren(children: List[Tree]): Tree = Equals(children(0), children(1))

  override def equals(obj: Any): Boolean = obj match {
    case Equals(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object Equals {
  def apply(left: Tree, right: Tree): Equals = new Equals(left, right)

  def unapply(arg: Equals): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}


class GreaterThan(val left: Tree, val right: Tree) extends BinaryLogicalOperator {
  override def replaceChildren(children: List[Tree]): Tree = GreaterThan(children(0), children(1))

  override def equals(obj: Any): Boolean = obj match {
    case GreaterThan(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object GreaterThan {
  def apply(left: Tree, right: Tree): GreaterThan = new GreaterThan(left, right)

  def unapply(arg: GreaterThan): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}


class GreaterThanOrEqual(val left: Tree, val right: Tree) extends BinaryLogicalOperator {
  override def replaceChildren(children: List[Tree]): Tree = GreaterThanOrEqual(children(0), children(1))

  override def equals(obj: Any): Boolean = obj match {
    case GreaterThanOrEqual(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object GreaterThanOrEqual {
  def apply(left: Tree, right: Tree): GreaterThanOrEqual = new GreaterThanOrEqual(left, right)

  def unapply(arg: GreaterThanOrEqual): Option[(Tree, Tree)] = Some((arg.left, arg.right))
}


class LessThan(val left: Tree, val right: Tree) extends BinaryLogicalOperator {
  override def replaceChildren(children: List[Tree]): Tree = LessThan(children(0), children(1))

  override def equals(obj: Any): Boolean = obj match {
    case LessThan(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object LessThan {
  def apply(left: Tree, right: Tree): Tree = new LessThan(left, right)

  def unapply(arg: LessThan): Option[(Tree, Tree)] = Some(arg.left, arg.right)
}


class LessThanOrEqual(val left: Tree, val right: Tree) extends BinaryLogicalOperator {
  override def replaceChildren(children: List[Tree]): Tree = LessThanOrEqual(children(0), children(1))

  override def equals(obj: Any): Boolean = obj match {
    case LessThanOrEqual(l, r) => this.left.equals(l) && this.right.equals(r)
    case _ => false
  }
}

object LessThanOrEqual {
  def apply(left: Tree, right: Tree): Tree = new LessThanOrEqual(left, right)

  def unapply(arg: LessThanOrEqual): Option[(Tree, Tree)] = Some(arg.left, arg.right)
}


/**
 * An expression that branches based on the value of an Option returned by the input expression.
 *
 * @param input  An expression.
 * @param ifSome A function to evaluate if the input expression returns a value.
 *               It must take a single argument which is the value of the option.
 * @param ifNone A function to evaluate if the input expression returns an empty option.
 *               It must take no arguments.
 */
class MapOption(val input: Tree, val ifSome: FunctionDef, val ifNone: FunctionDef) extends Tree {
  override def getChildren: Iterable[Tree] = Seq(this.input, this.ifSome, this.ifNone)

  override def replaceChildren(children: List[Tree]): Tree =
    MapOption(children(0), children(1).asInstanceOf[FunctionDef], children(2).asInstanceOf[FunctionDef])

  override def equals(obj: Any): Boolean = obj match {
    case MapOption(i, s, n) => this.input.equals(i) && this.ifSome.equals(s) && this.ifNone.equals(n)
    case _ => false
  }
}

object MapOption {
  def apply(input: Tree, ifSome: FunctionDef, ifNone: FunctionDef): MapOption = new MapOption(input, ifSome, ifNone)

  def unapply(arg: MapOption): Option[(Tree, FunctionDef, FunctionDef)] = Some((arg.input, arg.ifSome, arg.ifNone))
}
