package com.amazon.milan.program.internal

import com.amazon.milan.program
import com.amazon.milan.program._
import com.amazon.milan.typeutil.{TypeDescriptor, TypeInfoHost}

import scala.reflect.api.Trees
import scala.reflect.macros.whitebox

/**
 * Trait to enable macro bundles to convert scala expression tress to Milan expression trees.
 */
trait ConvertExpressionHost extends TypeInfoHost with FunctionReferenceHost with LiftableImpls {
  val c: whitebox.Context

  import c.universe._

  /**
   * Represents the context in which an expression is being generated.
   */
  private trait ExpressionContext {
    def isArgument(name: String): Boolean
  }

  /**
   * Standard expression context.
   */
  private class BaseExpressionContext extends ExpressionContext {
    override def isArgument(name: String): Boolean = false
  }

  /**
   * An [[ExpressionContext]] used when the current context is a function body.
   *
   * @param argNames The names of the input arguments to the function that contains the expression.
   */
  private class FunctionBodyExpressionContext(argNames: List[String],
                                              parentContext: ExpressionContext)
    extends ExpressionContext {

    override def isArgument(name: String): Boolean = {
      this.argNames.contains(name) || this.parentContext.isArgument(name)
    }
  }

  protected def abort(message: String): Nothing = c.abort(c.enclosingPosition, message)

  protected def warning(message: String): Unit = c.warning(c.enclosingPosition, message)

  /**
   * Gets a Milan [[FunctionDef]] expression from a scala AST representing a function definition.
   *
   * @param functionTree The scala AST of the function to convert.
   * @return The Milan function expression for the function.
   */
  def getMilanFunction(functionTree: c.universe.Tree): c.Expr[FunctionDef] = {
    try {
      functionTree match {
        case c.universe.Function(valDefs, body) =>
          getMilanFunction(valDefs.map(_.asInstanceOf[ValDef]), body.asInstanceOf[c.universe.Tree])

        case other =>
          abort(s"Function expression not supported: $other")
      }
    }
    catch {
      case ex: InvalidProgramException =>
        abort(s"Can't create Milan expression from '${showRaw(functionTree)}'. Error details: ${ex.getMessage}")
    }
  }

  /**
   * Gets a [[program.Tree]] containing the Milan expression tree for a scala expression.
   *
   * @param tree A scala expression tree.
   * @return A milan expression tree representing the scala expression.
   */
  def getMilanExpression(tree: c.universe.Tree): c.Expr[program.Tree] = {
    // Check if the scala expression is a function definition.
    // We don't do this check inside getMilanExpressionTree below because we don't support
    // nested function definitions.
    tree match {
      case c.universe.Function(valDefs, body) =>
        getMilanFunction(valDefs.map(_.asInstanceOf[ValDef]), body.asInstanceOf[c.universe.Tree])

      case _ =>
        val context = new BaseExpressionContext
        getMilanExpressionTree(context, tree)
    }
  }

  private def getMilanFunction(valDefs: List[ValDef],
                               body: c.universe.Tree): c.Expr[FunctionDef] = {
    val argNames = valDefs.map(_.name.toString)

    val context = new FunctionBodyExpressionContext(argNames, new BaseExpressionContext)
    val milanExpr = getMilanExpressionTree(context, body)
    val tree = q"new ${typeOf[FunctionDef]}($argNames, $milanExpr)"
    c.Expr[FunctionDef](tree)
  }

  private def getMilanExpressionTree(context: ExpressionContext,
                                     body: c.universe.Tree): c.Expr[program.Tree] = {
    val typeConversionTargetTypes = Set("Long", "Int", "String", "Double", "Float")

    def convert(value: Trees#Tree): c.Expr[program.Tree] =
      getMilanExpressionTree(context, value.asInstanceOf[c.universe.Tree])

    def convertList(trees: Seq[Trees#Tree]): List[c.Expr[program.Tree]] =
      trees.map(convert).toList

    def convertTypeDescriptor(tree: Trees#Tree): TypeDescriptor[_] =
      getTypeDescriptor(tree.asInstanceOf[c.universe.Tree])

    val outputTree =
      body match {
        case q"$operand == null" =>
          q"new ${typeOf[IsNull]}(${convert(operand)})"

        case q"$operand != null" =>
          q"new ${typeOf[Not]}(new ${typeOf[IsNull]}(${convert(operand)}))"

        case q"$leftOperand != $rightOperand" =>
          q"new ${typeOf[Not]}(new ${typeOf[Equals]}(${convert(leftOperand)}, ${convert(rightOperand)}))"

        case q"$leftOperand == $rightOperand" =>
          q"new ${typeOf[Equals]}(${convert(leftOperand)}, ${convert(rightOperand)})"

        case q"$leftOperand && $rightOperand" =>
          q"new ${typeOf[And]}(${convert(leftOperand)}, ${convert(rightOperand)})"

        case q"$leftOperand > $rightOperand" =>
          q"new ${typeOf[GreaterThan]}(${convert(leftOperand)}, ${convert(rightOperand)})"

        case q"$leftOperand < $rightOperand" =>
          q"new ${typeOf[LessThan]}(${convert(leftOperand)}, ${convert(rightOperand)})"

        case q"$leftOperand + $rightOperand" =>
          q"new ${typeOf[Plus]}(${convert(leftOperand)}, ${convert(rightOperand)})"

        case q"$leftOperand - $rightOperand" =>
          q"new ${typeOf[Minus]}(${convert(leftOperand)}, ${convert(rightOperand)})"

        case q"new $ty(..$args)" =>
          q"new ${typeOf[CreateInstance]}(${convertTypeDescriptor(ty)}, ${convertList(args)})"

        case Select(qualifier, TermName(name)) if name.startsWith("to") && typeConversionTargetTypes.contains(name.substring(2)) =>
          q"new ${typeOf[ConvertType]}(${convert(qualifier)}, ${TypeDescriptor.forTypeName[Any](name.substring(2))})"

        case Ident(TermName(name)) if context.isArgument(name) =>
          q"${SelectTerm(name)}"

        case Ident(TermName(name)) =>
          this.getConstantValueFromTerm(body.asInstanceOf[Ident])

        case s: Select =>
          convertSelect(context, s)

        case a: Apply =>
          this.getApplyFunctionExpressionTree(context, a)

        case If(cond, thenp, elsep) =>
          q"new ${typeOf[IfThenElse]}(${convert(cond)}, ${convert(thenp)}, ${convert(elsep)})"

        case Match(selector, cases) =>
          this.getMatchFunctionExpressionTree(context, selector, cases)

        case Literal(Constant(_)) =>
          q"${this.getConstantExpressionTree(body.asInstanceOf[c.universe.Literal].value)}"

        case Constant(_) =>
          q"${this.getConstantExpressionTree(body.asInstanceOf[c.universe.Constant])}"

        case invalid =>
          throw new InvalidProgramException(s"Can't create Milan expression from ${showRaw(invalid)}")
      }

    c.Expr[program.Tree](outputTree)
  }

  /**
   * Converts a select expression a Milan expression.
   *
   * @param select The select expression. This must be an Ident or a supported Select expression.
   * @return A tree that evaluates to a Milan expression representing the select.
   */
  private def convertSelect(context: ExpressionContext,
                            select: Select): c.universe.Tree = {
    val selectResult =
      select match {
        case Select(qualifier, TermName(name)) =>
          this.tryConvertSelect(context, qualifier, name)

        case invalid =>
          throw new InvalidProgramException(s"Can't create Milan expression from ${showRaw(invalid)}")
      }

    selectResult.fold(this.getConstantValueFromSelect(select))(tree => tree)
  }

  private def tryConvertSelect(context: ExpressionContext,
                               qualifier: Tree,
                               field: String): Option[c.universe.Tree] = {
    qualifier match {
      case Ident(TermName(name)) if context.isArgument(name) =>
        Some(q"${SelectField(SelectTerm(name), field)}")

      case Select(innerQualifier, TermName(innerField)) =>
        this.tryConvertSelect(context, innerQualifier, innerField).map(innerSelect =>
          q"new ${typeOf[SelectField]}($innerSelect, $field)"
        )

      case _ =>
        None
    }
  }

  private def getConstantValueFromSelect(select: ConvertExpressionHost.this.c.universe.Select): c.universe.Tree = {
    val outType = this.createTypeInfo[Any](select.tpe).toTypeDescriptor
    q"new ${typeOf[ConstantValue]}($select, $outType)"
  }

  /**
   * Gets a [[TypeDescriptor]] from a tree.
   *
   * @param tree An expression tree that references a type.
   * @return A [[TypeDescriptor]] for the type referenced by the tree.
   */
  private def getTypeDescriptor(tree: c.universe.Tree): TypeDescriptor[_] = {
    val typeName = this.getTypeName(tree)
    TypeDescriptor.forTypeName[Any](typeName)
  }

  /**
   * Gets an expression that evaluates to an [[ApplyFunction]] instance for a function described by its input arguments
   * and its body AST.
   *
   * @param context The context of the current expression.
   * @param apply   An [[Apply]] scala AST node.
   * @return A expression that evaluates to an [[ApplyFunction]] instance.
   */
  private def getApplyFunctionExpressionTree(context: ExpressionContext,
                                             apply: Apply): c.universe.Tree = {
    val functionReference = this.getFunctionReferenceFromTree(apply.fun)
    val argsList = this.getFunctionArguments(context, apply.args)

    if (functionReference.objectTypeName == "builtin") {
      this.getBuiltinFunction(functionReference, argsList)
    }
    else {
      val outType = this.createTypeInfo[Any](apply.tpe)
      q"new ${typeOf[ApplyFunction]}($functionReference, $argsList, ${outType.toTypeDescriptor})"
    }
  }

  /**
   * Gets a list of Tress that evaluate to Milan expressions representing the arguments to a function.
   *
   * @param context The context of the current expression.
   * @param args    A list of Trees representing the function arguments.
   * @return A Tree that evaluates to a list of (argument name, field name) tuples.
   */
  private def getFunctionArguments(context: ExpressionContext, args: List[c.universe.Tree]): List[c.Expr[program.Tree]] = {
    args.map(argTree => this.getMilanExpressionTree(context, argTree))
  }

  private def getMatchFunctionExpressionTree(context: ExpressionContext,
                                             selector: c.universe.Tree,
                                             cases: List[CaseDef]): c.universe.Tree = {
    // We only support selectors that are an Ident node referencing an argument.
    selector match {
      case Ident(TermName(argName)) if context.isArgument(argName) => ()
      case invalid => throw new InvalidProgramException(s"Invalid match selector '$invalid'.")
    }

    val Ident(TermName(parentArgName)) = selector

    if (cases.length != 1) {
      throw new InvalidProgramException("Only a single match case is supported.")
    }

    val CaseDef(casePattern, caseGuard, caseBody) = cases.head

    if (caseGuard.nonEmpty) {
      throw new InvalidProgramException("Case guards are not allowed.")
    }

    // match statements can be converted into a single Unpack expression because we only allow
    // a single match case and no case guards.
    val q"(..$caseArgs)" = casePattern
    val argNames = getCaseArgNames(caseArgs.map(_.asInstanceOf[c.universe.Tree])).toList

    // Create a nested context with the new case arg names.
    val caseContext = new FunctionBodyExpressionContext(argNames, context)

    val bodyNode = this.getMilanExpressionTree(caseContext, caseBody)

    q"new ${typeOf[Unpack]}(${SelectTerm(parentArgName)}, $argNames, $bodyNode)"
  }

  private def getCaseArgNames(caseArgs: Seq[c.universe.Tree]): Seq[String] = {
    caseArgs.map {
      case Bind(TermName(name), _) => name
      case Ident(TermName(name)) => name
      case Typed(Ident(termNames.WILDCARD), _) => "_"
      case invalid => throw new InvalidProgramException(s"Invalid case argument '$invalid' (${showRaw(invalid)}).")
    }
  }

  private def getConstantExpressionTree(value: c.universe.Constant): ConstantValue = {
    val typeDesc = createTypeInfo[Any](value.tpe).toTypeDescriptor
    new ConstantValue(value.value, typeDesc)
  }

  private def getConstantValueFromTerm(term: Ident): c.universe.Tree = {
    val typeInfo = createTypeInfo[Any](term.tpe)
    val typeDesc = typeInfo.toTypeDescriptor
    q"new ${typeOf[ConstantValue]}($term, $typeDesc)"
  }

  /**
   * Gets the Milan expression tree for a builtin function.
   */
  private def getBuiltinFunction(function: FunctionReference,
                                 args: List[c.Expr[program.Tree]]): c.universe.Tree = {
    function.functionName match {
      case "aggregation.sum" => q"new ${typeOf[Sum]}(${args.head})"
      case "aggregation.min" => q"new ${typeOf[Min]}(${args.head})"
      case "aggregation.max" => q"new ${typeOf[Max]}(${args.head})"
      case "aggregation.any" => q"new ${typeOf[First]}(${args.head})"
      case "aggregation.mean" => q"new ${typeOf[Mean]}(${args.head})"
      case "aggregation.argmin" => q"new ${typeOf[ArgMin]}(new ${typeOf[Tuple]}($args))"
      case "aggregation.argmax" => q"new ${typeOf[ArgMax]}(new ${typeOf[Tuple]}($args))"
      case unknown => throw new InvalidProgramException(s"Unrecognized built-in function '$unknown'.")
    }
  }
}
