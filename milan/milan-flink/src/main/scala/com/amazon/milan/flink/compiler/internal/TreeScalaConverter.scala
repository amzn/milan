package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.TypeUtil
import com.amazon.milan.flink.compiler.FlinkCompilationException
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program._
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.types.RecordIdFieldName
import com.amazon.milan.typeutil.{TypeDescriptor, types}

import scala.language.existentials


object TreeScalaConverter {
  private val jsonMapper = new ScalaObjectMapper()

  def getScalaAnonymousFunction(functionDef: FunctionDef, inputTypeName: String): String = {
    val inputType = TypeDescriptor.forTypeName[Any](inputTypeName)
    this.getScalaAnonymousFunction(functionDef, List(inputType))
  }

  def getScalaAnonymousFunction(functionDef: FunctionDef, inputType: TypeDescriptor[_]): String = {
    this.getScalaAnonymousFunction(functionDef, List(inputType))
  }

  def getScalaAnonymousFunction(functionDef: FunctionDef, inputTypes: List[TypeDescriptor[_]]): String = {
    val inputTypeNames = inputTypes.map(_.verboseName).mkString("(", ", ", ")")

    // First transform the function definition given the information from the context.
    // This will simplify things, for example removing Unpack expressions, allowing the conversion to be simpler.
    val transformed = try {
      ContextualTreeTransformer.transform(functionDef, inputTypes)
    }
    catch {
      case ex: Throwable =>
        throw new FlinkCompilationException(s"Error transforming function expression '$functionDef' with input types $inputTypeNames.", ex)
    }

    try {
      val argNamesAndTypes = functionDef.arguments.zip(inputTypes)
      val context = TreeScalaConverter.createContext(argNamesAndTypes)
      val argList = this.generateArgList(argNamesAndTypes)
      val body = context.getScalaCode(transformed.expr)

      s"$argList => $body"
    }
    catch {
      case ex: Exception =>
        throw new FlinkCompilationException(s"Error compiling function expression '$transformed' with input types $inputTypeNames.", ex)
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
        case ConvertType(expr, ty) => scala"$expr.to${ty.fullName}"
        case CreateInstance(ty, args) => scala"new $ty($args)"
        case Equals(left, right) => scala"($left == $right)"
        case FunctionReference(ty, name) => scala"$ty.$name"
        case GreaterThan(left, right) => scala"($left > $right)"
        case IfThenElse(cond, thenExpr, elseExpr) => scala"if ($cond) { $thenExpr } else { $elseExpr }"
        case IsNull(expr) => scala"($expr == null)"
        case LessThan(left, right) => scala"($left < $right)"
        case Minus(left, right) => scala"($left - $right)"
        case Not(expr) => scala"!$expr"
        case Plus(left, right) => scala"($left + $right)"
        case select: SelectExpression => this.generateSelectExpression(select)
        case Tuple(elements) => this.generateTuple(elements)
        case TypeDescriptor(typeName) => typeName
        case unpack: Unpack => this.generateUnpackExpression(unpack)

        case a: AggregateExpression => this.generateAggregateExpression(a)
        case f: FunctionDef => this.generateInlineAnonymousFunction(f)
        case s: String => s
        case l: List[_] => l.map(getScalaCode).mkString(", ")
        case o => throw new IllegalArgumentException(s"Can't convert expression '$o' to a scala expression.")
      }
    }

    def generateSelectExpression(select: SelectExpression): String = {
      val (output, _) = this.generateSelectExpressionAndContext(select)
      output
    }

    def generateSelectExpressionAndContext(select: SelectExpression): (String, ConversionContext) = {
      select match {
        case SelectTerm(termName) =>
          this.generateSelectTermAndContext(termName)

        case SelectField(qualifier, field) =>
          this.generateSelectFieldAndContext(qualifier, field)
      }
    }

    def generateSelectTermAndContext(name: String): (String, ConversionContext)

    def generateSelectFieldAndContext(qualifier: SelectExpression,
                                      field: String): (String, ConversionContext) = {
      val (qualifierOutput, qualifierContext) = this.generateSelectExpressionAndContext(qualifier)
      val (fieldOutput, fieldContext) = qualifierContext.generateSelectTermAndContext(field)
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
      if (elements.isEmpty) {
        "new org.apache.flink.api.java.tuple.Tuple0()"
      }
      else {
        val tupleClassName = TypeUtil.getTupleClassName(elements.length)
        scala"new $tupleClassName(${elements.map(getScalaCode).mkString(", ")})"
      }
    }

    private def generateAggregateExpression(expr: AggregateExpression): String = {
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
      val newFunctionBody = TreeScalaConverter.renameArgumentInTree(expr.expr, argumentName, newArgName)
      val innerFunction = FunctionDef(List(newArgName), newFunctionBody)

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
        throw new FlinkCompilationException(s"Term '$name' does not match the function argument '${this.argName}'.")
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
          throw new FlinkCompilationException(s"Field named '$name' not found.")

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
        throw new FlinkCompilationException(s"Term '$name' does not match the function argument '${this.argName}'.")
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
          throw new FlinkCompilationException(s"Field '$name' not found.")
        }

        val fieldType = this.tupleType.fields(fieldIndex).fieldType
        (s"($fieldIndex).asInstanceOf[${fieldType.fullName}]", new RecordFieldConversionContext(fieldType))
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
          throw new FlinkCompilationException(s"Unknown argument name '$name'.")
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


  private def createContext(argNamesAndTypes: List[(String, TypeDescriptor[_])]): ConversionContext = {
    if (argNamesAndTypes.length == 1) {
      val (argName, argType) = argNamesAndTypes.head
      this.createContext(argName, argType)
    }
    else {
      val argContexts = argNamesAndTypes.map { case (name, ty) => (name, this.createContext(name, ty)) }
      new MultipleInputConversionContext(argContexts)
    }
  }

  private def createContext(argName: String, argType: TypeDescriptor[_]): ConversionContext = {
    // If the record type is a tuple with named fields then this is a tuple stream whose records are stored as
    // ArrayRecord objects.
    if (argType.isTuple && argType.fields.nonEmpty) {
      new ArrayArgumentConversionContext(argName, argType)
    }
    else {
      new RecordArgumentConversionContext(argName, argType)
    }
  }

  /**
   * Gets an escaped version of a string.
   */
  private def escape(str: String): String = {
    jsonMapper.writeValueAsString(str)
  }

  private def generateArgList(argNamesAndTypes: List[(String, TypeDescriptor[_])]): String = {
    argNamesAndTypes.map { case (name, ty) => this.generateArgDeclaration(name, ty) }.mkString("(", ", ", ")")
  }

  private def generateArgDeclaration(argName: String, argType: TypeDescriptor[_]): String = {
    // If the record type is a tuple with named fields then this is a tuple stream whose records are stored as
    // ArrayRecord objects.
    if (argType.isTuple && argType.fields.nonEmpty) {
      s"$argName: ${ArrayRecord.typeName}"
    }
    else {
      s"$argName: ${argType.fullName}"
    }
  }

  /**
   * Gets a copy of an expression tree where references to the specified argument name are replaced with a new
   * argument name.
   *
   * @param expr         An expression tree.
   * @param originalName The name of the argument to rename.
   * @param newName      The name of the renamed argument in the output tree.
   * @return An expression tree that is identical to the input expression tree but with any references to the original
   *         argument name changed to the new name.
   */
  private def renameArgumentInTree(expr: Tree, originalName: String, newName: String): Tree = {
    val newChildren = expr.getChildren.map(child => this.renameArgumentInTree(child, originalName, newName))

    expr match {
      case SelectTerm(name) if name == originalName =>
        SelectTerm(newName)

      case _ =>
        expr.replaceChildren(newChildren.toList)
    }
  }
}
