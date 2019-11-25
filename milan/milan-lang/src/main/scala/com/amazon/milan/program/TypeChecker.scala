package com.amazon.milan.program

import com.amazon.milan.types.RecordIdFieldName
import com.amazon.milan.typeutil.{GroupedStreamTypeDescriptor, JoinedStreamsTypeDescriptor, StreamTypeDescriptor, TypeDescriptor, types}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.annotation.tailrec


object TypeChecker {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Apply type checking to a [[FunctionDef]] expression tree.
   * This will modify the tpe property of the tree and any children that have not had a return type assigned.
   *
   * @param functionDef A [[FunctionDef]] expression tree.
   * @param inputTypes  The types of the inputs that make up the context of the expression tree.
   */
  def typeCheck(functionDef: FunctionDef, inputTypes: List[TypeDescriptor[_]]): Unit = {
    // Short circuit if the tree has already been typechecked.
    if (functionDef.tpe != null) {
      return
    }

    val FunctionDef(argNames, body) = functionDef

    if (argNames.length != inputTypes.length) {
      throw new InvalidProgramException(s"Invalid number of function arguments; expected ${inputTypes.length}, found ${argNames.length}.")
    }

    val argInfo = argNames.zip(inputTypes).map {
      case (argName, argType) => ArgInfo(argName, argType)
    }

    typeCheckFunctionBody(body, argInfo)

    // The function definition has the same return type as the body.
    functionDef.tpe = body.tpe
  }

  /**
   * Apply type checking to a [[FunctionDef]] expression tree.
   * This will modify the tpe property of the tree and any children that have not had a return type assigned.
   *
   * @param functionDef A [[FunctionDef]] expression tree.
   */
  def typeCheck(functionDef: FunctionDef): Unit = {
    // Short circuit if the tree has already been typechecked.
    if (functionDef.tpe != null) {
      return
    }

    val FunctionDef(_, body) = functionDef

    typeCheckExpression(body)
    functionDef.tpe = body.tpe
  }

  /**
   * Performs type checking on an expression tree whose root is a [[GraphNodeExpression]].
   *
   * @param expression     The expression to type check.
   * @param inputNodeTypes A map of node IDs to types of stream nodes that are external to the expression tree.
   */
  def typeCheck(expression: GraphNodeExpression, inputNodeTypes: Map[String, StreamTypeDescriptor]): Unit = {
    this.logger.debug(s"Type-checking expression '$expression'.")

    // Typecheck the input graph nodes first. Any function defs in this node will be functions of these inputs, so we'll
    // need their types to typecheck the function defs.
    val inputTypes = this.typeCheckInputsAndGetFunctionInputTypes(expression, inputNodeTypes)

    expression.getChildren.foreach {
      case f: FunctionDef =>
        TypeChecker.typeCheck(f, inputTypes)

      case f: FieldDefinition =>
        TypeChecker.typeCheck(f.expr, inputTypes)
        f.tpe = f.expr.tpe

      case _ =>
        ()
    }

    if (expression.tpe == null) {
      expression.tpe = this.getTreeType(expression, inputNodeTypes)
    }
  }

  /**
   * Apply type checking to an expression tree.
   * This will modify the tpe property of the tree and any children that have not had a return type assigned.
   * Because no argument information is supplied, any Select nodes in the tree must already have been typechecked
   * otherwise this function will fail.
   *
   * @param expression An expression tree.
   */
  def typeCheckExpression(expression: Tree): Unit = {
    this.logger.debug(s"Type-checking expression '$expression'.")

    expression.getChildren.foreach(child => this.typeCheckExpression(child))

    // Short circuit if the tree has already been typechecked.
    if (expression.tpe != null) {
      return
    }

    expression.tpe = getTreeType(expression)
  }

  /**
   * Gets the result type of a [[SelectExpression]] node, given a context type.
   *
   * @param select  A [[SelectExpression]] node.
   * @param context The type that represents that context of the node. The root term name of the node will be a field
   *                of this type.
   * @return A [[TypeDescriptor]] describing the result type of the node.
   */
  def getSelectExpressionType(select: SelectExpression, context: TypeDescriptor[_]): TypeDescriptor[_] = {
    select match {
      case SelectTerm(termName) if context.fieldExists(termName) =>
        context.getField(termName).fieldType

      case SelectTerm(termName) if termName == RecordIdFieldName && context.isTupleStreamType =>
        types.String

      case SelectTerm(termName) =>
        throw new InvalidProgramException(s"A field named '$termName' does not exists for type '${context.fullName}'.")

      case SelectField(qualifier, name) =>
        this.getSelectExpressionType(qualifier, context).getField(name).fieldType
    }
  }

  /**
   * Type check the body of a [[FunctionDef]] expression tree.
   */
  private def typeCheckFunctionBody(body: Tree, argInfo: List[ArgInfo]): Unit = {
    this.logger.debug(s"Type-checking expression '$body'.")

    val nestedArgInfo = this.getNestedArgInfo(body, argInfo)
    body.getChildren.foreach(child => this.typeCheckFunctionBody(child, nestedArgInfo))

    // Short circuit if the tree has already been typechecked.
    if (body.tpe != null) {
      return
    }

    // Perform the type check, which depends on the exact type of the tree.
    // Many tree types specify the return type in the class definition, so here we only need to include the ones that
    // don't do this.
    val tpe = body match {
      case s: SelectExpression => this.getSelectExpressionType(argInfo, s)
      case Unpack(_, _, unpackBody) => unpackBody.tpe
      case ArgAggregateExpression(e) => e.elements(1).tpe
      case AggregateExpression(e) => e.tpe
      case _ => this.getTreeType(body)
    }

    body.tpe = tpe
  }

  /**
   * Gets a list of [[ArgInfo]] containing the [[ArgInfo]] that arise from an expression tree that may introduce a new
   * scope, layered on top of an existing list of [[ArgInfo]].
   *
   * @param tree    An expression tree that causes a new scope to be entered.
   * @param argInfo The [[ArgInfo]] list for the current scope.
   * @return A list of [[ArgInfo]] representing the arguments available to be referenced in the new scope.
   */
  private def getNestedArgInfo(tree: Tree, argInfo: List[ArgInfo]): List[ArgInfo] = {
    tree match {
      case Unpack(value, names, _) =>
        // Unpack introduces a new scope as it assigns names to the fields of the element types of the referenced stream.
        val valueFieldTypes = argInfo.find(arg => arg.argName == value.termName).get.ty.genericArguments

        if (names.length != valueFieldTypes.length) {
          throw new InvalidProgramException(s"Mismatch between tuple element count (${valueFieldTypes.length}) and unpack argument names (${names.length}).")
        }

        val unpackArgInfo = names.zip(valueFieldTypes)
          .map { case (name, fieldType) => ArgInfo(name, fieldType) }

        val nameSet = names.toSet

        // Filter any name collisions and append the unpacked arguments.
        argInfo.filter(info => !nameSet.contains(info.argName)) ++ unpackArgInfo

      case _ =>
        argInfo
    }
  }

  /**
   * Gets the return type of a tree.
   */
  private def getTreeType(tree: Tree): TypeDescriptor[_] = {
    // Perform the type check, which depends on the exact type of the tree.
    // Many tree types specify the return type in the class definition, so here we only need to include the ones that
    // don't do this.
    tree match {
      case IfThenElse(_, thenExpr, elseExpr) =>
        if (!areTypesCompatible(thenExpr.tpe, elseExpr.tpe)) {
          throw new TypeCheckException(s"If and Else branches must have compatible types. Found '${thenExpr.tpe.fullName}' and '${elseExpr.tpe.fullName}'.")
        }
        thenExpr.tpe

      case BinaryMathOperator(left, right) =>
        if (!areTypesCompatible(left.tpe, right.tpe)) {
          throw new TypeCheckException(s"Operands of binary operations must have compatible types. Found '${left.tpe.fullName}' and '${right.tpe.fullName}'.")
        }
        left.tpe

      case Tuple(elements) =>
        val elementTypes = elements.map(_.tpe)
        TypeDescriptor.createTuple[Any](elementTypes)

      case _ =>
        throw new TypeCheckException(s"Unable to perform type checking for trees of type '${tree.expressionType}' without argument information.")
    }
  }

  /**
   * Gets the [[TypeDescriptor]] object for an argument.
   *
   * @param argInfo A list of [[ArgInfo]] representing the available arguments.
   * @param argName The name of an argument.
   * @return The [[TypeDescriptor]] object contained in the [[ArgInfo]] with the specified name.
   */
  private def getArgType(argInfo: List[ArgInfo], argName: String): TypeDescriptor[_] = {
    argInfo.find(_.argName == argName) match {
      case Some(info) =>
        info.ty

      case None =>
        val argNames = argInfo.map(_.argName).mkString(", ")
        throw new InvalidProgramException(s"No argument named '$argName'. Available arguments: ($argNames).")
    }
  }

  private def getSelectExpressionType(argInfo: List[ArgInfo], select: SelectExpression): TypeDescriptor[_] = {
    val argName = select.getRootTermName
    val argType = this.getArgType(argInfo, argName)

    select match {
      case _: SelectTerm => argType
      case _ => this.getSelectExpressionType(select.trimRootTermName(), argType)
    }
  }

  private def areTypesCompatible(type1: TypeDescriptor[_], type2: TypeDescriptor[_]): Boolean = {
    canCoerce(type1, type2) || canCoerce(type2, type1)
  }

  private def canCoerce(sourceType: TypeDescriptor[_], destType: TypeDescriptor[_]): Boolean = {
    if (sourceType.isNumeric && destType.isNumeric) {
      true
    }
    else {
      sourceType == destType
    }
  }

  /**
   * Typechecks the [[GraphNodeExpression]] children of a [[GraphNodeExpression]] and returns the argument types of
   * functions that are executed in the scope of the expression.
   * Depending on the expression the argument types will be a combination of the input stream type(s) and the group key
   * type. For example for an expression like MapRecord(GroupBy(source, keyFunction)) the argument types are
   * (group key type, source type).
   */
  private def typeCheckInputsAndGetFunctionInputTypes(expr: GraphNodeExpression,
                                                      inputNodeTypes: Map[String, StreamTypeDescriptor]): List[TypeDescriptor[_]] = {
    val graphNodeInputs = expr.getChildren.filter(_.isInstanceOf[GraphNodeExpression]).map(_.asInstanceOf[GraphNodeExpression])

    graphNodeInputs.foreach(input => this.typeCheck(input, inputNodeTypes))

    this.getFunctionArgumentTypes(expr, inputNodeTypes)
  }

  /**
   * Gets the types of the arguments to any functions that are executed in the scope of a graph expression.
   * This will not necessarily be the types of the direct inputs to the expression. For example, a MapRecord expression
   * may have as input a FullJoin, in which case this method will return the types of the input streams to the FullJoin.
   */
  @tailrec
  private def getFunctionArgumentTypes(input: GraphNodeExpression,
                                       inputNodeTypes: Map[String, StreamTypeDescriptor]): List[TypeDescriptor[_]] = {
    input match {
      case Ref(nodeId) =>
        List(inputNodeTypes(nodeId).recordType)

      case JoinNodeExpression(left, right, _) =>
        List(left.recordType, right.recordType)

      case WindowedLeftJoin(left, right) =>
        List(left.recordType, right.getInputRecordType)

      case TimeWindowExpression(GroupBy(source, _), _, _, _) =>
        this.getRecordTypes(source.tpe)

      case GroupingExpression(source, _) =>
        this.getRecordTypes(source.tpe)

      case Filter(source, _) =>
        this.getRecordTypes(source.tpe)

      case UniqueBy(source, _) =>
        this.getFunctionArgumentTypes(source, inputNodeTypes)

      case MapNodeExpression(source) =>
        this.getMapFunctionArgumentTypes(source, inputNodeTypes)
    }
  }

  /**
   * Gets the types of the arguments of a map function.
   * If the source of the map function is a grouping expression then the group key will be the first argument.
   */
  private def getMapFunctionArgumentTypes(source: GraphNodeExpression,
                                          inputNodeTypes: Map[String, StreamTypeDescriptor]): List[TypeDescriptor[_]] = {
    source match {
      case m: MapNodeExpression =>
        this.getRecordTypes(m.tpe)

      case UniqueBy(GroupingExpression(groupSource, keyFunctionDef), _) =>
        // Map functions that map the output of a unique-by have the group key as one of the inputs.
        // This must be treated separately from other GroupingExpression expressions because the key function of
        // UniqueBy returns the unique key, not the group key.
        List(keyFunctionDef.tpe) ++ this.getMapSourceRecordTypes(groupSource, inputNodeTypes)

      case LatestBy(source, _, _) =>
        List(source.recordType)

      case GroupingExpression(groupSource, keyFunctionDef) =>
        // Map functions that map the output of a group-by or window have the group key as one of the inputs.
        List(keyFunctionDef.tpe) ++ this.getMapSourceRecordTypes(groupSource, inputNodeTypes)

      case o: GraphNodeExpression =>
        this.getFunctionArgumentTypes(o, inputNodeTypes)
    }
  }

  /**
   * Gets the record types of the source of a map expression.
   */
  @tailrec
  private def getMapSourceRecordTypes(source: GraphNodeExpression,
                                      inputNodeTypes: Map[String, StreamTypeDescriptor]): List[TypeDescriptor[_]] = {
    source match {
      case m: MapNodeExpression =>
        this.getRecordTypes(m.tpe)

      case GroupingExpression(groupSource, _) =>
        this.getMapSourceRecordTypes(groupSource, inputNodeTypes)

      case o: GraphNodeExpression =>
        this.getFunctionArgumentTypes(o, inputNodeTypes)
    }
  }

  /**
   * Gets the record types for a type that represents a stream-like type.
   * For example, for a join expression, the record types are the records types of the left and right input streams.
   */
  private def getRecordTypes(streamType: TypeDescriptor[_]): List[TypeDescriptor[_]] = {
    streamType match {
      case StreamTypeDescriptor(recordType) => List(recordType)
      case JoinedStreamsTypeDescriptor(leftRecordType, rightRecordType) => List(leftRecordType, rightRecordType)
      case GroupedStreamTypeDescriptor(recordType) => List(recordType)
    }
  }

  /**
   * Gets the result type of a [[GraphNodeExpression]] expression tree.
   */
  private def getTreeType(expr: GraphNodeExpression, inputNodeTypes: Map[String, StreamTypeDescriptor]): TypeDescriptor[_] = {
    expr match {
      case Ref(nodeId) =>
        inputNodeTypes(nodeId)

      case MapRecord(_, f) =>
        types.stream(f.tpe)

      case MapFields(_, fields) =>
        val recordType = TypeDescriptor.createNamedTuple[Any](fields.map(field => (field.fieldName, field.expr.tpe)))
        types.stream(recordType)

      case Filter(source, _) =>
        source.tpe

      case UniqueBy(source, _) =>
        source.tpe

      case g: GroupingExpression =>
        types.groupedStream(g.getInputRecordType)

      case JoinNodeExpression(left, right, _) =>
        types.joinedStreams(left.recordType, right.recordType)

      case WindowedLeftJoin(left, right) =>
        types.joinedStreams(left.recordType, right.getInputRecordType)
    }
  }

  private case class ArgInfo(argName: String, ty: TypeDescriptor[_])

}


class TypeCheckException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}
