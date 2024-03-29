package com.amazon.milan.compiler.scala.trees

import com.amazon.milan.compiler.scala.trees.TreeExtractor.{ExtractionContext, ExtractionResult}
import com.amazon.milan.program.{AggregateExpression, FunctionDef, InvalidProgramException, SelectTerm, Tree, Tuple, TupleElement, TypeChecker, ValueDef}

object AggregateFunctionTreeExtractor {
  /**
   * Splits a Milan function into multiple functions, by taking branches under calls to aggregate functions.
   * For example "r => foo(r.a) + bar(r.b)" will be split into "r => foo(r.a)" and "r => bar(r.b)".
   * The output functions will have the same arguments as the input function.
   *
   * This version of the method operates on aggregate functions of keyed streams.
   *
   * @param function A function definition. This function should already have been typechecked. The first argument to
   *                 this function should be the group key.
   * @return A list of function definitions corresponding to the input arguments to any aggregate function calls
   *         in the input function.
   */
  def getAggregateInputFunctionsWithKey(function: FunctionDef): List[FunctionDef] = {
    // Check to make sure we were given a valid aggregate function definition.
    com.amazon.milan.lang.internal.ProgramValidation.validateSelectFromGroupByFunction(function)

    // The first argument to the aggregate function should be the group key.
    // The group key can't be used inside an aggregate function, so we need to remove it from the arguments
    // of the functions we return.
    val inputFunctionArguments = function.arguments.tail

    this.getAggregateInputFunctions(function, inputFunctionArguments)
  }

  /**
   * Splits a Milan function into multiple functions, by taking branches under calls to aggregate functions.
   * For example "r => foo(r.a) + bar(r.b)" will be split into "r => foo(r.a)" and "r => bar(r.b)".
   * The output functions will have the same arguments as the input function.
   *
   * This version of the method operates on aggregate functions of unkeyed streams.
   *
   * @param function A function definition. This function should already have been typechecked.
   * @return A list of function definitions corresponding to the input arguments to any aggregate function calls
   *         in the input function.
   */
  def getAggregateInputFunctionsWithoutKey(function: FunctionDef): List[FunctionDef] = {
    this.getAggregateInputFunctions(function, function.arguments)
  }

  /**
   * Splits a Milan function into multiple functions, by taking branches under calls to aggregate functions.
   * For example "r => foo(r.a) + bar(r.b)" will be split into "r => foo(r.a)" and "r => bar(r.b)".
   * The output functions will have the same arguments as the input function.
   *
   * @param function               A function definition. This function should already have been typechecked.
   * @param inputFunctionArguments The arguments to the input function that can be referenced from the aggregate
   *                               functions.
   * @return A list of function definitions corresponding to the input arguments to any aggregate function calls
   *         in the input function.
   */
  private def getAggregateInputFunctions(function: FunctionDef, inputFunctionArguments: List[ValueDef]): List[FunctionDef] = {
    val aggregateFunctionArguments = this.getAggregateFunctionArguments(function.body)

    val functions = aggregateFunctionArguments.map(expr => FunctionDef(inputFunctionArguments, expr))

    // Typecheck the functions so that we know their return types.
    // This assumes that the aggregate function had already been typechecked, otherwise we won't know the type of
    // any SelectField or SelectTerm nodes.
    functions.foreach(TypeChecker.typeCheck)

    functions
  }

  /**
   * Gets the [[AggregateExpression]] objects for the aggregate functions applied in a Milan function.
   *
   * @param function A function definition.
   * @return A list of the [[AggregateExpression]] objects for the aggregate functions applied in the input function.
   */
  def getAggregateExpressions(function: FunctionDef): List[AggregateExpression] = {
    this.getAggregateExpressionsInTree(function.body)
  }

  /**
   * Converts a Milan function, that at the top level is made up of zero or more associative binary operations, into a
   * function of a tuple whose elements are the types of those operations.
   * For example, if foo and bar are functions that both return Int, "r: Record => foo(r.a) + bar(r.b)" will be
   * converted into "value: (Int, Int) => value._1 + value._2".
   *
   * This version of the method operates on functions that take the group key as the first argument.
   *
   * @param function A function definition. The first argument should be the group key.
   * @return The converted function definition.
   */
  def getResultTupleToOutputFunctionWithKey(function: FunctionDef): FunctionDef = {
    val FunctionDef(args, body) = function
    val outputBody = this.resultCombinerExtractor.extract(body, ExtractionContext(Some("0"))).extracted.get

    // The first argument in the input function is the group key, we need to keep that in the output function.
    val outputFunction = FunctionDef.create(List(args.head.name, "result"), outputBody)
    outputFunction.tpe = function.tpe

    outputFunction
  }

  /**
   * Converts a Milan function, that at the top level is made up of zero or more associative binary operations, into a
   * function of a tuple whose elements are the types of those operations.
   * For example, if foo and bar are functions that both return Int, "r: Record => foo(r.a) + bar(r.b)" will be
   * converted into "value: (Int, Int) => value._1 + value._2".
   *
   * This version of the method operates on functions that do not take the group key as the first argument.
   *
   * @param function A function definition.
   * @return The converted function definition.
   */
  def getResultTupleToOutputFunctionWithoutKey(function: FunctionDef): FunctionDef = {
    val FunctionDef(args, body) = function
    val outputBody = this.resultCombinerExtractor.extract(body, ExtractionContext(Some("0"))).extracted.get

    // The first argument in the input function is the group key, we need to keep that in the output function.
    val outputFunction = FunctionDef.create(List("result"), outputBody)
    outputFunction.tpe = function.tpe

    outputFunction
  }

  /**
   * Gets a list of the arguments to the aggregate function calls that are found in an expression tree.
   *
   * @param expr An expression tree.
   * @return A list of the arguments of any aggregate function calls in the tree.
   *         There is a 1:1 mapping between the elements of this list and the elements of the list returned by
   *         [[getAggregateExpressionsInTree]].
   *         If a function call has more than one argument, the resulting [[Tree]] in the returned list will be a
   *         [[Tuple]] whose elements are the function arguments.
   */
  private def getAggregateFunctionArguments(expr: Tree): List[Tree] = {
    // Do a depth-first search for aggregate functions and return their arguments.
    if (isAggregateExpression(expr)) {
      val args = expr.getChildren.toList

      // If the function takes anything but one argument then we need to wrap those arguments in a Tuple expression
      // so that we can create a FunctionDef that returns a tuple of the argument values.
      if (args.length != 1) {
        List(new Tuple(args))
      }
      else {
        args
      }
    }
    else {
      expr.getChildren.flatMap(getAggregateFunctionArguments).toList
    }
  }

  /**
   * Gets a list of [[AggregateExpression]] objects from the aggregate function calls that are found in an expression
   * tree.
   *
   * @param expr An expression tree.
   * @return A list of the [[AggregateExpression]] objects from the aggregate function calls.
   *         There is a 1:1 mapping between the elements of this list and the elements of the list returned by
   *         [[getAggregateFunctionArguments]].
   */
  private def getAggregateExpressionsInTree(expr: Tree): List[AggregateExpression] = {
    // Do a depth-first search for aggregate function calls and return their FunctionReferences.
    expr match {
      case agg: AggregateExpression =>
        List(agg)

      case _ =>
        expr.getChildren.flatMap(this.getAggregateExpressionsInTree).toList
    }
  }

  /**
   * Gets whether an expression tree represents a call to an aggregate function.
   */
  private def isAggregateExpression(expr: Tree): Boolean =
    expr.isInstanceOf[AggregateExpression]

  /**
   * A [[TreeExtractor]] that extracts the portion of an expression tree that combines the outputs of
   * aggregate function calls.
   */
  private val resultCombinerExtractor = new TreeExtractor {
    override def extract(tree: Tree, context: ExtractionContext): ExtractionResult = {
      // Aggregate function calls are replaced with SelectField expressions that reference fields in the input tuple.
      // Other expressions are left as-is with their child expressions replaced with potentially modified trees.
      tree match {
        case FunctionDef(_, body) =>
          this.extract(body, context)

        case t if isAggregateExpression(t) =>
          // The context contains an integer in the arg name, which is the tuple element we will use
          // to replace this node in the tree.
          val argIndex = context.argumentName.get.toInt
          val replacement = new TupleElement(SelectTerm("result"), argIndex)
          val newContext = ExtractionContext(Some((argIndex + 1).toString))
          ExtractionResult(Some(replacement), None, newContext)

        case t if t.getChildren.isEmpty =>
          ExtractionResult(Some(t), None, context)

        case _ =>
          val scanResults =
            tree.getChildren.scanLeft((context, None: Option[Tree])) {
              case ((currentContext, _), childTree) =>
                val r = this.extract(childTree, currentContext)
                (r.context, r.extracted)
            }
              .drop(1)
              .toList

          val newChildren = scanResults.map { case (_, c) => c.get }
          val (newContext, _) = scanResults.last
          val outputTree = tree.replaceChildren(newChildren)
          ExtractionResult(Some(outputTree), None, newContext)
      }
    }

    override protected def combineContexts(contexts: List[ExtractionContext]): Option[ExtractionContext] = {
      // This should never be called, because we don't call any of the map functions in TreeExtractorUtil.
      throw new InvalidProgramException("You shouldn't be here.")
    }
  }
}
