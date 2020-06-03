package com.amazon.milan.program

import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, FieldDescriptor, GroupedStreamTypeDescriptor, JoinedStreamsTypeDescriptor, StreamTypeDescriptor, TypeDescriptor, types}

import scala.annotation.tailrec


class TypeCheckException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message: String) {
    this(message, null)
  }
}


object TypeChecker {
  /**
   * Apply type checking to a [[FunctionDef]] expression tree.
   * This will modify the tpe property of the tree and any children that have not had a return type assigned.
   *
   * @param functionDef A [[FunctionDef]] expression tree.
   * @param inputTypes  The types of the inputs that make up the context of the expression tree.
   */
  def typeCheck(functionDef: FunctionDef, inputTypes: List[TypeDescriptor[_]]): Unit = {
    val context = Context(inputTypes, Map(), functionDef.arguments.map(arg => arg.name -> arg.tpe).toMap)
    this.typeCheck(context)(functionDef)
  }

  /**
   * Apply type checking to a [[FunctionDef]] expression tree.
   * This will modify the tpe property of the tree and any children that have not had a return type assigned.
   *
   * @param functionDef A [[FunctionDef]] expression tree.
   */
  def typeCheck(functionDef: FunctionDef): Unit = {
    val context = Context(List(), Map(), functionDef.arguments.map(arg => arg.name -> arg.tpe).toMap)

    val FunctionDef(_, body) = functionDef

    this.typeCheck(context)(body)
    functionDef.tpe = body.tpe
  }

  /**
   * Performs type checking on an expression tree whose root is a [[StreamExpression]].
   *
   * @param expression The expression to type check.
   * @param nodeTypes  A map of node IDs to types of stream nodes that are external to the expression tree.
   */
  def typeCheck(expression: StreamExpression, nodeTypes: Map[String, StreamTypeDescriptor]): Unit = {
    val context = Context(List(), nodeTypes, Map())
    this.typeCheck(context)(expression)
  }

  /**
   * Performs type checking on an expression in a given context.
   *
   * @param context The context of the type checking.
   * @param expr    The expression to typecheck.
   */
  private def typeCheck(context: Context)(expr: Tree): Unit = {
    // We're doing a depth-first traversal of the tree with two phases at each node.
    // If the first phase we typecheck any child expressions that don't depend on the context introduced by this
    // expression.
    // In the second phase we typecheck the remaining child expressions, using any new context introduced by this
    // expression.
    // After that we have all the information we need to determine the type of this expression.

    // As above, first typecheck child expressions that this expression depends on.
    this.getContextChildren(expr).foreach(this.typeCheck(context))

    // Get the new context and typecheck all of the child expressions.
    this.typeCheckChildrenWithNewContexts(expr, context)

    expr.tpe = this.getTreeType(expr, context)
  }

  /**
   * Gets child trees that may inform the context of the parent tree and are not dependent on that context introduced by
   * the parent tree.
   */
  private def getContextChildren(tree: Tree): List[Tree] = {
    tree match {
      case SingleInputStreamExpression(input) => List(input)
      case TwoInputStreamExpression(left, right) => List(left, right)
      case Unpack(target, _, _) => List(target)
      case _ => List()
    }
  }

  /**
   * Gets child trees that depend on the context of the parent tree, along with the context to use for
   * typechecking that child tree.
   */
  private def typeCheckChildrenWithNewContexts(expr: Tree, parentContext: Context): Unit = {
    val allChildren = expr.getChildren.toList

    val primaryContext = this.getNewContext(expr, parentContext)

    // For some expressions, the first argument(s) determine the context of the expression.
    expr match {
      case SumBy(_, argExpr, outputExpr) =>
        this.typeCheck(primaryContext)(argExpr)

        val outputExprContext = parentContext.withContextTypes(primaryContext.contextTypes :+ argExpr.tpe)
        this.typeCheck(outputExprContext)(outputExpr)

      case _: SingleInputStreamExpression =>
        allChildren.drop(1).foreach(this.typeCheck(primaryContext))

      case _: JoinExpression =>
        allChildren.drop(2).foreach(this.typeCheck(primaryContext))

      case _: Unpack =>
        allChildren.drop(1).foreach(this.typeCheck(primaryContext))

      case _ =>
        allChildren.foreach(this.typeCheck(primaryContext))
    }
  }

  /**
   * Gets the context introduced by an expression.
   *
   * @param expr    An expression.
   * @param context The context of the expression.
   * @return A new context introduced by the expression. This may be the input context if the expression is not one that
   *         introduces a new context.
   */
  private def getNewContext(expr: Tree, context: Context): Context = {
    expr match {
      case FunctionDef(args, _) =>
        // FunctionDef introduces a new scope by assigning names to the context types.
        if (args.length != context.contextTypes.length) {
          throw new InvalidProgramException(s"Invalid number of function arguments for '$expr'; expected ${context.contextTypes.length}, found ${args.length}.")
        }

        val functionArgInfo = args.map(_.name).zip(context.contextTypes).toMap
        context.addValues(functionArgInfo)

      case Unpack(term, names, _) =>
        if (!term.tpe.isTuple) {
          throw new InvalidProgramException(s"The target of an Unpack operation must be a tuple.")
        }

        // Unpack introduces a new scope as it assigns names to the fields of the element types of the referenced stream.
        val valueFieldTypes = term.tpe.genericArguments

        if (names.length != valueFieldTypes.length) {
          throw new InvalidProgramException(s"Mismatch between tuple element count (${valueFieldTypes.length}) and unpack argument names (${names.length}).")
        }

        val unpackArgInfo = names.zip(valueFieldTypes).toMap
        context.addValues(unpackArgInfo)

      case s: StreamExpression =>
        val expressionContextTypes = this.getFunctionArgumentTypes(s, context)
        context.withContextTypes(expressionContextTypes)

      case _ =>
        context
    }
  }

  /**
   * Gets the types of the arguments to any functions that are executed in the scope of a graph expression.
   * This will not necessarily be the types of the direct inputs to the expression. For example, a MapRecord expression
   * may have as input a FullJoin, in which case this method will return the types of the input streams to the FullJoin.
   *
   * @param scopingExpr A [[StreamExpression]] that defines the scope of functions being executed.
   * @param context     The current type checking context.
   */
  private def getFunctionArgumentTypes(scopingExpr: Tree,
                                       context: Context): List[TypeDescriptor[_]] = {
    scopingExpr match {
      case Ref(nodeId) =>
        List(context.getStreamType(nodeId).recordType)

      case ExternalStream(nodeId, _, _) =>
        List(context.getStreamType(nodeId).recordType)

      case TimeWindowExpression(GroupBy(source, _), _, _, _) =>
        this.getRecordTypes(source, context)

      case Aggregate(source, _) =>
        this.getMapFunctionArgumentTypes(source, context)

      case StreamMap(source, _) =>
        this.getMapFunctionArgumentTypes(source, context)

      case FlatMap(source, mapFunction) =>
        this.getFlatMapFunctionArgumentTypes(source, mapFunction, context)

      case JoinExpression(left, right, _) =>
        this.getRecordTypes(left, context) ++ this.getRecordTypes(right, context)

      case LeftWindowedJoin(left, right) =>
        this.getRecordTypes(left, context) ++ this.getRecordTypes(right, context)

      case SingleInputStreamExpression(source) =>
        this.getRecordTypes(source, context)

      case _: Union =>
        List()
    }
  }

  /**
   * Gets the types of the arguments of a map function, based on the expression that represents the data source of the
   * map function.
   * If the source of the map function is a grouping expression then the group key will be the first argument.
   */
  private def getMapFunctionArgumentTypes(mapInput: Tree, context: Context): List[TypeDescriptor[_]] = {
    mapInput match {
      case m: StreamMap =>
        this.getRecordTypes(m.streamType)

      case GroupingExpression(groupSource, keyFunctionDef) =>
        // Map functions that map the output of a group-by or window have the group key as one of the inputs.
        List(keyFunctionDef.tpe) ++ this.getRecordTypes(groupSource, context)

      case t: SelectTerm if t.tpe.isStream =>
        List(t.tpe.asStream.recordType)

      case s: StreamExpression =>
        getRecordTypes(s, context)
    }
  }

  /**
   * Gets the types of the arguments of a map function, based on the expression that represents the data source of the
   * map function.
   * If the source of the map function is a grouping expression then the group key will be the first argument.
   */
  private def getFlatMapFunctionArgumentTypes(mapInput: Tree,
                                              mapFunction: FunctionDef,
                                              context: Context): List[TypeDescriptor[_]] = {
    mapInput match {
      case GroupingExpression(_, _) if mapFunction.arguments.length == 1 =>
        List(mapInput.tpe)

      case GroupingExpression(_, keyFunctionDef) if mapFunction.arguments.length == 2 =>
        List(keyFunctionDef.tpe, mapInput.tpe)

      case LeftWindowedJoin(left, right) =>
        List(this.getRecordType(left, context), this.getRecordType(right, context).toIterable)
    }
  }

  /**
   * Gets the record types of an expression that returns a stream.
   */
  private def getRecordTypes(source: Tree, context: Context): List[TypeDescriptor[_]] = {
    source match {
      case GroupingExpression(groupSource, _) =>
        this.getRecordTypes(groupSource, context)

      case t: SelectTerm if t.tpe.isStream =>
        List(t.tpe.asStream.recordType)

      case t: SelectTerm =>
        List(t.tpe)

      case Ref(refNodeId) if context.streamExists(refNodeId) =>
        List(context.getStreamType(refNodeId).recordType)

      case ExternalStream(refNodeId, _, _) if context.streamExists(refNodeId) =>
        List(context.getStreamType(refNodeId).recordType)

      case JoinExpression(left, right, _) =>
        this.getRecordTypes(left, context) ++ this.getRecordTypes(right, context)

      case s: StreamExpression =>
        this.getRecordTypes(s.streamType)
    }
  }

  /**
   * Gets the record type of an expression that returns a stream.
   */
  private def getRecordType(expr: Tree, context: Context): TypeDescriptor[_] = {
    this.getRecordTypes(expr, context) match {
      case List(recordType) =>
        recordType

      case _ =>
        throw new TypeCheckException(s"Expected a stream with a single record type.")
    }
  }

  /**
   * Gets the record types for a type that represents a stream-like type.
   * For example, for a join expression, the record types are the records types of the left and right input streams.
   */
  private def getRecordTypes(streamType: StreamTypeDescriptor): List[TypeDescriptor[_]] = {
    streamType match {
      case DataStreamTypeDescriptor(recordType) => List(recordType)
      case JoinedStreamsTypeDescriptor(leftRecordType, rightRecordType) => List(leftRecordType, rightRecordType)
      case GroupedStreamTypeDescriptor(recordType) => List(recordType)
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
   * Gets the return type of a tree.
   */
  @tailrec
  private def getTreeType(tree: Tree, context: Context): TypeDescriptor[_] = {
    // Perform the type check, which depends on the exact type of the tree.
    // Many tree types specify the return type in the class definition, so here we only need to include the ones that
    // don't do this.
    tree match {
      case t if t.tpe != null => t.tpe

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

      case ArgAggregateExpression(e) => e.elements(1).tpe
      case UnaryAggregateExpression(e) => e.tpe
      case FunctionDef(_, body) => body.tpe
      case NamedField(_, expr) => expr.tpe
      case NamedFields(fields) => this.createNamedTuple(fields)
      case SelectField(qualifier, name) => qualifier.tpe.getField(name).fieldType
      case SelectTerm(name) => context.getValueType(name)
      case TupleElement(target, index) => target.tpe.genericArguments(index)
      case Unpack(_, _, unpackBody) => unpackBody.tpe
      case ValueDef(name, _) => context.getValueType(name)

      case Aggregate(_, aggFunction) => types.stream(aggFunction.tpe)
      case ExternalStream(nodeId, _, _) => context.getStreamType(nodeId)
      case Filter(source, _) => this.getTreeType(source, context)
      case FlatMap(_, mapFunction) => types.stream(mapFunction.tpe)
      case LeftWindowedJoin(left, right) => types.joinedStreams(this.getRecordType(left, context), this.getRecordType(right, context))
      case GroupingExpression(source, _) => types.groupedStream(this.getRecordType(source, context))
      case JoinExpression(left, right, _) => types.joinedStreams(this.getRecordType(left, context), this.getRecordType(right, context))
      case Last(s) => s.tpe
      case StreamMap(_, mapFunction) => types.stream(mapFunction.tpe)
      case SlidingRecordWindow(source, _) => types.groupedStream(this.getRecordType(source, context))
      case ArgCompareExpression(source, _) => this.getTreeType(source, context)

      case _ =>
        throw new TypeCheckException(s"Unable to perform type checking for trees of type '${tree.expressionType}'.")
    }
  }

  private def createNamedTuple(fields: List[NamedField]): TypeDescriptor[_] = {
    TypeDescriptor.createNamedTuple(fields.map(f => (f.fieldName, f.tpe)))
  }


  private case class Context(contextTypes: List[TypeDescriptor[_]],
                             streams: Map[String, StreamTypeDescriptor],
                             values: Map[String, TypeDescriptor[_]]) {
    def getValueType(name: String): TypeDescriptor[_] = {
      this.values.get(name) match {
        case None => throw new InvalidProgramException(s"No value named '$name' in scope.")
        case Some(ty) => ty
      }
    }

    def getStreamType(nodeId: String): StreamTypeDescriptor = {
      this.streams.get(nodeId) match {
        case None => throw new InvalidProgramException(s"No stream with Id '$nodeId' in scope.")
        case Some(stream) => stream
      }
    }

    def valueExists(name: String): Boolean =
      this.values.contains(name)

    def streamExists(nodeId: String): Boolean =
      this.streams.contains(nodeId)

    /**
     * Gets a [[Context]] that is equivalent to this [[Context]] but with additional named values added.
     */
    def addValues(valuesToAdd: Map[String, TypeDescriptor[_]]): Context = {
      Context(this.contextTypes, this.streams, this.values ++ valuesToAdd)
    }

    /**
     * Gets a [[Context]] that is equivalent to this [[Context]] but with additional named values added based on
     * a set of [[FieldDescriptor]] objects.
     */
    def addValueFields(fields: List[FieldDescriptor[_]]): Context = {
      this.addValues(fields.map(f => f.name -> f.fieldType).toMap)
    }

    /**
     * Gets a [[Context]] that is equivalent to this [[Context]] with the contextTypes replaced.
     */
    def withContextTypes(newContextTypes: List[TypeDescriptor[_]]): Context = {
      Context(newContextTypes, this.streams, this.values)
    }
  }

}
