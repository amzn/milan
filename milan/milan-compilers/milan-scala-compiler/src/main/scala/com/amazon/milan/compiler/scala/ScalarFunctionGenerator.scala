package com.amazon.milan.compiler.scala

import java.util.UUID

import com.amazon.milan.compiler.scala.internal.AggregateFunctions
import com.amazon.milan.program.{And, ApplyFunction, ConstantValue, ConvertType, CreateInstance, Equals, FunctionDef, FunctionReference, GreaterThan, GreaterThanOrEqual, IfThenElse, InvalidProgramException, IsNull, LessThan, LessThanOrEqual, Minus, NamedFields, Not, Plus, SelectExpression, SelectField, SelectTerm, Tree, TreeOperations, Tuple, TupleElement, TypeChecker, UnaryAggregateExpression, Unpack, ValueDef}
import com.amazon.milan.serialization.MilanObjectMapper
import com.amazon.milan.typeutil.{TypeDescriptor, types}

import scala.language.existentials


object ScalarFunctionGenerator {
  val default = new ScalarFunctionGenerator(new DefaultTypeEmitter, new IdentityTreeTransformer)
}


case class FunctionParts(arguments: CodeBlock, returnType: CodeBlock, body: CodeBlock)


/**
 * Generates Scala functions of scalar values (i.e. not of Milan streams).
 *
 * @param typeEmitter     A [[TypeEmitter]] that is used to emit types into Scala code.
 * @param treeTransformer A [[TreeTransformer]] that is used to transform Milan expression trees before using them to
 *                        generate Scala functions.
 */
class ScalarFunctionGenerator(typeEmitter: TypeEmitter,
                              treeTransformer: TreeTransformer) {
  private val jsonMapper = new MilanObjectMapper()

  /**
   * Gets Scala code that defines an anonymous function that implements a [[FunctionDef]].
   *
   * @param functionDef A [[FunctionDef]] that has been typechecked.
   * @return A string containing Scala code that implements the function.
   */
  def getScalaAnonymousFunction(functionDef: FunctionDef): String = {
    validateFunctionArgsHaveTypes(functionDef)

    val (argList, body) = this.getArgListAndFunctionBody(functionDef)
    s"$argList => $body"
  }

  /**
   * Gets Scala code that defines a function that implements a [[FunctionDef]].
   *
   * @param functionName The name of the function to generate.
   * @param functionDef  A [[FunctionDef]] that has been typechecked.
   * @return A string containing the Scala function definition, including the def statement.
   */
  def getScalaFunctionDef(functionName: String, functionDef: FunctionDef): String = {
    val parts = this.getScalaFunctionParts(functionDef)
    s"""def $functionName${parts.arguments}: ${parts.returnType} = {
       |  ${parts.body}
       |}
       |""".stripMargin
  }

  def getScalaFunctionParts(functionDef: FunctionDef): FunctionParts = {
    validateFunctionArgsHaveTypes(functionDef)

    val (argList, body) = this.getArgListAndFunctionBody(functionDef)
    val returnTypeName = this.typeEmitter.getTypeFullName(functionDef.tpe)

    FunctionParts(CodeBlock(argList), CodeBlock(returnTypeName), CodeBlock(body))
  }

  private def getArgListAndFunctionBody(functionDef: FunctionDef): (String, String) = {
    val inputTypeNames = functionDef.arguments.map(arg => this.typeEmitter.getTypeFullName(arg.tpe)).mkString("(", ", ", ")")

    // First transform the function definition given the information from the context.
    // This will simplify things, for example removing Unpack expressions, allowing the conversion to be simpler.
    val transformed = try {
      this.treeTransformer.transform(functionDef)
    }
    catch {
      case ex: Throwable =>
        throw new ScalaConversionException(s"Error transforming function expression '$functionDef' with input types $inputTypeNames.", ex)
    }

    try {
      val context = this.createContextForArguments(functionDef.arguments)
      val argList = this.generateArgList(functionDef.arguments)

      TypeChecker.typeCheck(transformed)
      val body = context.getScalaCode(transformed.body)

      (argList, body)
    }
    catch {
      case ex: Exception =>
        throw new ScalaConversionException(s"Error converting function expression '$transformed' with input types $inputTypeNames.", ex)
    }
  }


  protected trait ConversionContext {

    /**
     * A custom string interpolator we will use to construct scala code from Milan expression trees.
     *
     * @param sc A string context.
     */
    implicit class TreeScalaInterpolator(sc: StringContext) {
      def scala(subs: Any*): String = {
        val partsIterator = sc.parts.iterator
        val subsIterator = subs.iterator

        val sb = new StringBuilder(partsIterator.next())
        while (subsIterator.hasNext) {
          sb.append(getScalaCode(subsIterator.next()))
          sb.append(partsIterator.next())
        }

        sb.toString()
      }
    }

    def getScalaCode(tree: Any): String = {
      tree match {
        case ApplyFunction(fun, args, _) => scala"$fun($args)"
        case And(left, right) => scala"($left && $right)"
        case ConstantValue(value, ty) => this.generateConstant(value, ty)
        case ConvertType(expr, ty) => scala"$expr.to${typeEmitter.getTypeFullName(ty)}"
        case CreateInstance(ty, args) => scala"new $ty($args)"
        case Equals(left, right) => scala"($left == $right)"
        case FunctionReference(ty, name) => scala"$ty.$name"
        case GreaterThan(left, right) => scala"($left > $right)"
        case GreaterThanOrEqual(left, right) => scala"($left >= $right)"
        case IfThenElse(cond, thenExpr, elseExpr) => scala"if ($cond) { $thenExpr } else { $elseExpr }"
        case IsNull(expr) => scala"($expr == null)"
        case LessThan(left, right) => scala"($left < $right)"
        case LessThanOrEqual(left, right) => scala"($left <= $right)"
        case NamedFields(fields) => this.generateTuple(fields.map(_.expr))
        case Minus(left, right) => scala"($left - $right)"
        case Not(expr) => scala"!$expr"
        case Plus(left, right) => scala"($left + $right)"
        case select: SelectExpression => this.generateSelectExpression(select)
        case Tuple(elements) => this.generateTuple(elements)
        case TupleElement(target, index) => scala"$target.${typeEmitter.getTupleElementField(index)}"
        case ty: TypeDescriptor[_] => typeEmitter.getTypeFullName(ty)
        case unpack: Unpack => this.generateUnpackExpression(unpack)

        case a: UnaryAggregateExpression => this.generateAggregateExpression(a)
        case f: FunctionDef => this.generateInlineAnonymousFunction(f)
        case ValueDef(name, ty) => scala"$name: $ty"
        case s: String => s
        case l: List[_] => l.map(getScalaCode).mkString(", ")
        case o => throw new IllegalArgumentException(s"Can't convert expression '$o' to a scala expression.")
      }
    }

    /**
     * Generates the code for a [[SelectExpression]].
     */
    def generateSelectExpression(select: SelectExpression): String = {
      select match {
        case SelectTerm(termName) =>
          val (output, _) = this.generateSelectTermAndContext(termName)
          output

        case selectField: SelectField =>
          val (output, _) = this.generateSelectFieldAndContext(selectField)
          output
      }
    }

    /**
     * Generates the code for an expression and returns a [[ConversionContext]] that can be used
     * to append further select expressions to the output.
     */
    def generateSelectQualifierAndContext(qualifier: Tree): (String, ConversionContext) = {
      qualifier match {
        case SelectTerm(termName) =>
          this.generateSelectTermAndContext(termName)

        case selectField: SelectField =>
          this.generateSelectFieldAndContext(selectField)

        case o =>
          val context = createContextForType(o.tpe)
          val code = this.getScalaCode(o)
          (code, context)
      }
    }

    /**
     * Generates the code for a [[SelectTerm]] with the specified term name, and returns a [[ConversionContext]]
     * that can be used to append further select expressions to the output.
     */
    def generateSelectTermAndContext(name: String): (String, ConversionContext)

    /**
     * Generates the code for a [[SelectField]] expression and returns a [[ConversionContext]] that can be used to
     * append further select expressions to the output.
     */
    def generateSelectFieldAndContext(select: SelectField): (String, ConversionContext) = {
      val (qualifierOutput, qualifierContext) = this.generateSelectQualifierAndContext(select.qualifier)
      val (fieldOutput, fieldContext) = qualifierContext.generateSelectTermAndContext(select.fieldName)
      (qualifierOutput + fieldOutput, fieldContext)
    }

    def generateInlineAnonymousFunction(functionDef: FunctionDef): String = {
      val FunctionDef(args, body) = functionDef

      // Convert the function body using a new context that has no argument information.
      val newContext = processContext(new BasicArgumentConversionContext)
      val bodyCode = newContext.getScalaCode(body)

      scala"($args) => $bodyCode"
    }

    private def generateTuple(elements: List[Tree]): String = {
      val tupleClassName = typeEmitter.getTupleClassName(elements.length)
      scala"$tupleClassName(${elements.map(getScalaCode).mkString(", ")})"
    }

    private def generateAggregateExpression(expr: UnaryAggregateExpression): String = {
      // If we're converting an aggregate expression into scala code then we must be inside a function of an iterable of
      // some record type.
      // The inner expression will be some function of individual records. We need to create an anonymous function that
      // implements the inner expression and then pass that to a function that implements the aggregate operation.

      val argumentNames = this.getArgumentNames(expr.expr)
      if (argumentNames.size != 1) {
        throw new InvalidProgramException("An aggregate operation must use exactly one input argument to the function.")
      }

      val argumentName = argumentNames.head
      val newArgName = "__item__"
      val newFunctionBody = TreeOperations.renameArgumentInTree(expr.expr, argumentName, newArgName)
      val innerFunction = FunctionDef.create(List(newArgName), newFunctionBody)

      scala"${AggregateFunctions.getAggregateExpressionFunctionFullName(expr)}($argumentName.map($innerFunction))"
    }

    private def generateConstant(value: Any, ty: TypeDescriptor[_]): String = {
      if (ty == types.String) {
        escape(value.asInstanceOf[String])
      }
      else {
        value.toString
      }
    }

    private def generateUnpackExpression(unpack: Unpack): String = {
      val Unpack(target, valueNames, body) = unpack

      val unpackContext = processContext(new UnpackConversionContext(valueNames, this))
      val bodyCode = unpackContext.getScalaCode(body)
      scala"$target match { case ($valueNames) => " + bodyCode + " }"
    }

    /**
     * Gets the names of the arguments referenced by expressions in a tree.
     */
    private def getArgumentNames(expr: Tree): Set[String] = {
      expr match {
        case SelectTerm(name) =>
          Set(name)

        case _ =>
          if (expr.getChildren.isEmpty) {
            Set()
          }
          else {
            expr.getChildren.map(this.getArgumentNames).reduce((x, y) => x ++ y)
          }
      }
    }
  }


  /**
   * A conversion context that accepts any term and writes them out verbatim.
   */
  private class BasicArgumentConversionContext extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      // Write any name we're given, but any subsequent terms must have a "." in front of them.
      (name, processContext(new BasicFieldConversionContext))
    }
  }


  /**
   * A conversion context that is a child context of [[BasicArgumentConversionContext]], which  accepts any term and
   * writes it verbatim with a dot in front.
   */
  private class BasicFieldConversionContext extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      (s".$name", this)
    }
  }


  /**
   * Context that accepts a single term name which refers to a named argument that is an object of the specified type.
   */
  private class ArgumentConversionContext(argName: String, argType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (name != this.argName) {
        throw new ScalaConversionException(s"Term '$name' does not match the function argument '${this.argName}'.")
      }

      (name, processContext(new FieldConversionContext(argType)))
    }
  }


  /**
   * Context where term names refer to fields of an object of a specified type.
   * This context is introduced by [[SelectField]] expressions.
   */
  private class FieldConversionContext(recordType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      this.recordType.tryGetField(name) match {
        case None if this.recordType.fields.isEmpty =>
          // We don't have any field information in the record type.
          // We'll still generate the code and hope it compiles, but we don't have anything reasonable to put in the
          // output context.
          (s".$name", processContext(new FieldConversionContext(types.Nothing)))

        case None =>
          throw new ScalaConversionException(s"Field named '$name' not found.")

        case Some(field) =>
          (s".$name", processContext(new FieldConversionContext(field.fieldType)))
      }
    }
  }


  /**
   * Context where a term name may refer to one of several named arguments, each with their own corresponding context.
   */
  private class MultipleInputConversionContext(args: List[(String, ConversionContext)]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      this.args.find { case (argName, _) => argName == name } match {
        case Some((_, context)) =>
          context.generateSelectTermAndContext(name)

        case None =>
          throw new ScalaConversionException(s"Unknown argument name '$name'.")
      }
    }
  }


  /**
   * Context that accepts terms that refer to unpack target values.
   */
  private class UnpackConversionContext(valueNames: List[String], parentContext: ConversionContext) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (this.valueNames.contains(name)) {
        (name, processContext(new BasicFieldConversionContext))
      }
      else {
        this.parentContext.generateSelectTermAndContext(name)
      }
    }
  }


  protected def processContext(context: ConversionContext): ConversionContext =
    context

  /**
   * Creates a [[ConversionContext]] introduced by a set of named values.
   */
  protected def createContextForArguments(values: List[ValueDef]): ConversionContext = {
    if (values.length == 1) {
      this.createContextForArgument(values.head)
    }
    else {
      val argContexts = values.map(valueDef => (valueDef.name, this.createContextForArgument(valueDef)))
      processContext(new MultipleInputConversionContext(argContexts))
    }
  }

  /**
   * Creates a [[ConversionContext]] introduced by a named value.
   */
  protected def createContextForArgument(valueDef: ValueDef): ConversionContext = {
    processContext(new ArgumentConversionContext(valueDef.name, valueDef.tpe))
  }

  /**
   * Creates a [[ConversionContext]] introduced by a value of the specified type.
   */
  protected def createContextForType(contextType: TypeDescriptor[_]): ConversionContext = {
    processContext(new FieldConversionContext(contextType))
  }

  /**
   * Gets an escaped version of a string.
   */
  private def escape(str: String): String = {
    jsonMapper.writeValueAsString(str)
  }

  private def generateArgList(args: List[ValueDef]): String = {
    args.map(arg => this.generateArgDeclaration(arg.name, arg.tpe)).mkString("(", ", ", ")")
  }

  private def generateArgDeclaration(argName: String, argType: TypeDescriptor[_]): String = {
    val validArgName = this.getValidArgName(argName)
    s"$validArgName: ${this.typeEmitter.getTypeFullName(argType)}"
  }

  private def getValidArgName(argName: String): String = {
    if (argName == "_") {
      "notused_" + UUID.randomUUID().toString.substring(0, 4)
    }
    else {
      argName
    }
  }

  private def validateFunctionArgsHaveTypes(functionDef: FunctionDef): Unit = {
    if (functionDef.arguments.exists(arg => arg.tpe == null)) {
      throw new InvalidProgramException(s"One or more arguments are missing type information.")
    }
  }

}
