package com.amazon.milan.flink.internal

import java.util.UUID

import com.amazon.milan.flink.TypeUtil
import com.amazon.milan.flink.generator.{CodeBlock, FlinkGeneratorException}
import com.amazon.milan.flink.typeutil._
import com.amazon.milan.program.{And, ApplyFunction, ConstantValue, ConvertType, CreateInstance, Equals, FunctionDef, FunctionReference, GreaterThan, GreaterThanOrEqual, IfThenElse, InvalidProgramException, IsNull, LessThan, LessThanOrEqual, Minus, NamedFields, Not, Plus, SelectExpression, SelectField, SelectTerm, Tree, TreeOperations, Tuple, TupleElement, TypeChecker, UnaryAggregateExpression, Unpack, ValueDef}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.types._
import com.amazon.milan.typeutil.{TypeDescriptor, types}

import scala.language.existentials


object TreeScalaConverter {
  def getScalaAnonymousFunction(functionDef: FunctionDef, inputTypeName: String): String = {
    TreeScalaConverter.forScalaTypes.getScalaAnonymousFunction(functionDef, inputTypeName)
  }

  def getScalaAnonymousFunction(functionDef: FunctionDef, inputType: TypeDescriptor[_]): String = {
    TreeScalaConverter.forScalaTypes.getScalaAnonymousFunction(functionDef.withArgumentTypes(List(inputType)))
  }

  def getScalaAnonymousFunction(functionDef: FunctionDef): String = {
    TreeScalaConverter.forScalaTypes.getScalaAnonymousFunction(functionDef)
  }

  def getScalaAnonymousFunction(functionDef: FunctionDef, inputTypes: List[TypeDescriptor[_]]): String = {
    TreeScalaConverter.forScalaTypes.getScalaAnonymousFunction(functionDef.withArgumentTypes(inputTypes))
  }

  val forFlinkTypes = new TreeScalaConverter(new FlinkTypeEmitter)

  val forScalaTypes = new TreeScalaConverter(new DefaultTypeEmitter)
}


case class FunctionParts(arguments: CodeBlock, returnType: CodeBlock, body: CodeBlock)


class TreeScalaConverter(typeEmitter: TypeEmitter) {
  private val jsonMapper = new ScalaObjectMapper()

  def getScalaAnonymousFunction(functionDef: FunctionDef, inputTypeName: String): String = {
    val inputType = TypeDescriptor.forTypeName[Any](inputTypeName)
    this.getScalaAnonymousFunction(functionDef.withArgumentTypes(List(inputType)))
  }

  def getScalaAnonymousFunction(functionDef: FunctionDef): String = {
    validateFunctionArgsHaveTypes(functionDef)

    val (argList, body) = this.getArgListAndFunctionBody(functionDef)
    s"$argList => $body"
  }

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
    val inputTypeNames = functionDef.arguments.map(_.tpe.verboseName).mkString("(", ", ", ")")

    // First transform the function definition given the information from the context.
    // This will simplify things, for example removing Unpack expressions, allowing the conversion to be simpler.
    val transformed = try {
      ContextualTreeTransformer.transform(functionDef)
    }
    catch {
      case ex: Throwable =>
        throw new FlinkGeneratorException(s"Error transforming function expression '$functionDef' with input types $inputTypeNames.", ex)
    }

    try {
      val context = this.createContext(functionDef.arguments)
      val argList = this.generateArgList(functionDef.arguments)

      TypeChecker.typeCheck(transformed)
      val body = context.getScalaCode(transformed.body)

      (argList, body)
    }
    catch {
      case ex: Exception =>
        throw new FlinkGeneratorException(s"Error compiling function expression '$transformed' with input types $inputTypeNames.", ex)
    }
  }


  private trait ConversionContext {

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
        case TypeDescriptor(typeName) => typeName
        case unpack: Unpack => this.generateUnpackExpression(unpack)

        case a: UnaryAggregateExpression => this.generateAggregateExpression(a)
        case f: FunctionDef => this.generateInlineAnonymousFunction(f)
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
          val context = createContext(o.tpe)
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
      val newContext = new BasicArgumentConversionContext
      val bodyCode = newContext.getScalaCode(body)

      scala"($args) => $bodyCode"
    }

    private def generateTuple(elements: List[Tree]): String = {
      val tupleClassName = TypeUtil.getTupleClassName(elements.length)
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

      val unpackContext = new UnpackConversionContext(valueNames, this)
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
      (name, new BasicFieldConversionContext)
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
   * Context that accepts a single term name which refers to a named argument that is an object stream type.
   */
  private class RecordArgumentConversionContext(argName: String, argType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (name != this.argName) {
        throw new FlinkGeneratorException(s"Term '$name' does not match the function argument '${this.argName}'.")
      }

      (name, new RecordFieldConversionContext(argType))
    }
  }


  /**
   * Context where term names refer to fields in an object stream.
   */
  private class RecordFieldConversionContext(recordType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      this.recordType.tryGetField(name) match {
        case None if this.recordType.fields.isEmpty =>
          // We don't have any field information in the record type.
          // We'll still generate the code and hope it compiles, but we don't have anything reasonable to put in the
          // output context.
          (s".$name", new RecordFieldConversionContext(types.Nothing))

        case None =>
          throw new FlinkGeneratorException(s"Field named '$name' not found.")

        case Some(field) =>
          (s".$name", new RecordFieldConversionContext(field.fieldType))
      }
    }
  }


  /**
   * Context that accepts a single term name which refers to a named argument that is a tuple stream type.
   */
  private class ArrayArgumentConversionContext(argName: String, argType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (name != this.argName) {
        throw new FlinkGeneratorException(s"Term '$name' does not match the function argument '${this.argName}'.")
      }

      (name, new ArrayFieldConversionContext(argType))
    }
  }


  /**
   * Context where term names refer to fields in a tuple stream.
   */
  private class ArrayFieldConversionContext(tupleType: TypeDescriptor[_]) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (name == RecordIdFieldName) {
        // RecordId is a special field for tuple streams, because it's a property of the ArrayRecord class rather than
        // being present in the fields array itself.
        (s".$name", new RecordFieldConversionContext(types.String))
      }
      else {
        val fieldIndex = this.tupleType.fields.takeWhile(_.name != name).length

        if (fieldIndex >= this.tupleType.fields.length) {
          throw new FlinkGeneratorException(s"Field '$name' not found.")
        }

        val fieldType = this.tupleType.fields(fieldIndex).fieldType
        (s"($fieldIndex).asInstanceOf[${typeEmitter.getTypeFullName(fieldType)}]", new RecordFieldConversionContext(fieldType))
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
          throw new FlinkGeneratorException(s"Unknown argument name '$name'.")
      }
    }
  }


  /**
   * Context that accepts terms that refer to unpack target values.
   */
  private class UnpackConversionContext(valueNames: List[String], parentContext: ConversionContext) extends ConversionContext {
    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      if (this.valueNames.contains(name)) {
        (name, new BasicFieldConversionContext)
      }
      else {
        this.parentContext.generateSelectTermAndContext(name)
      }
    }
  }


  /**
   * Creates a [[ConversionContext]] introduced by a set of named values.
   */
  private def createContext(values: List[ValueDef]): ConversionContext = {
    if (values.length == 1) {
      this.createContext(values.head)
    }
    else {
      val argContexts = values.map(valueDef => (valueDef.name, this.createContext(valueDef)))
      new MultipleInputConversionContext(argContexts)
    }
  }

  /**
   * Creates a [[ConversionContext]] introduced by a named value.
   */
  private def createContext(valueDef: ValueDef): ConversionContext = {
    // If the record type is a tuple with named fields then this is a tuple stream whose records are stored as
    // ArrayRecord objects.
    if (valueDef.tpe.isTupleRecord) {
      new ArrayArgumentConversionContext(valueDef.name, valueDef.tpe)
    }
    else {
      new RecordArgumentConversionContext(valueDef.name, valueDef.tpe)
    }
  }

  /**
   * Creates a [[ConversionContext]] introduced by a value of the specified type.
   */
  private def createContext(contextType: TypeDescriptor[_]): ConversionContext = {
    if (contextType.isTupleRecord) {
      new ArrayFieldConversionContext(contextType)
    }
    else {
      new RecordFieldConversionContext(contextType)
    }
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
}
