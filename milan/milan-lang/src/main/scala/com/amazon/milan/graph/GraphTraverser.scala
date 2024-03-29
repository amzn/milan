package com.amazon.milan.graph

import com.amazon.milan.program._
import com.amazon.milan.typeutil.{GroupedStreamTypeDescriptor, TupleTypeDescriptor, TypeDescriptor}

import scala.annotation.tailrec


/**
 * A base class that can be used when traversal of the dependency graph between stream expressions is required.
 * Inheritors provide a node type that they use to hold additional information they want to gather during the traversal.
 *
 * @tparam T The node type of the constructed graph.
 * @note The child expressions of a Milan StreamExpression can be both dependents of and dependencies of a given
 *       stream expression. For example, the [[StreamMap]] expression has two children. The first is the input stream
 *       to the map expression, this is a dependency of [[StreamMap]]. The second child expression is a function of the
 *       input stream; this child expression depends on the input stream, and is considered a dependent of the
 *       [[StreamMap]] expression.
 */
abstract class GraphTraverser[T] {
  private var nodes: Map[String, TraverseNode] = Map.empty

  protected def createNode(expr: StreamExpression, context: Context): T

  /**
   * Registers a dependency relationship.
   *
   * @param parent The parent expression.
   * @param child  A child of the parent expression.
   */
  protected def registerDependency(parent: T, child: T): Unit

  /**
   * Begins graph traversal at the specified expression.
   *
   * @param expr A stream expression.
   */
  protected def traverse(expr: StreamExpression): Unit = {
    this.traverse(expr, _ => false)
  }

  /**
   * Begins graph traversal at the specified expression, with the ability to control traversal into expressions.
   *
   * @param expr                 A stream expression.
   * @param isBoundaryExpression A function that identifies expressions that should not be traversed into.
   */
  protected def traverse(expr: StreamExpression,
                         isBoundaryExpression: StreamExpression => Boolean): Unit = {
    val context = Context(Map.empty, None, None)
    this.traverse(expr, context, isBoundaryExpression)
  }

  /**
   * Gets the [[TraverseNode]] for an expression, traversing into the expression if it has not already been
   * visited.
   *
   * @param expr                 The expression to traverse.
   * @param context              The traversal context.
   * @param isBoundaryExpression Returns true if an expression should not be included in the traversal.
   * @return The [[TraverseNode]] corresponding to the expression.
   */
  @tailrec
  private def traverse(expr: Tree,
                       context: Context,
                       isBoundaryExpression: StreamExpression => Boolean): Option[TraverseNode] = {
    // StreamExpressions become nodes, and the inputs to those expressions are added as children of those nodes.
    // The exceptions are Map and FlatMap when the input is a grouped stream; these don't become nodes themselves,
    // but the stream expressions inside their map functions do. The Map and FlatMap expressions become the context
    // of any nodes created when traversing the map function bodies.
    expr match {
      case SelectTerm(name) =>
        context.nodeTerms.get(name)

      case streamExpr: StreamExpression if isBoundaryExpression(streamExpr) =>
        None

      case streamExpr: StreamExpression =>
        this.traverseStreamExpression(streamExpr, context, isBoundaryExpression)

      case FunctionDef(_, body) =>
        this.traverse(body, context, isBoundaryExpression)

      case _ =>
        None
    }
  }

  /**
   * Gets the [[TraverseNode]] for a [[StreamExpression]], traversing into the expression if it has not already been
   * visited.
   *
   * @param streamExpr           The [[StreamExpression]] to traverse.
   * @param context              The traversal context.
   * @param isBoundaryExpression Returns true if an expression should not be included in the traversal.
   * @return The [[TraverseNode]] corresponding to the expression.
   */
  private def traverseStreamExpression(streamExpr: StreamExpression,
                                       context: Context,
                                       isBoundaryExpression: StreamExpression => Boolean): Option[TraverseNode] = {
    this.nodes.get(streamExpr.nodeId) match {
      case Some(node) =>
        // We've already traversed this node so don't do it again.
        Some(node)

      case None =>
        this.traverseStreamExpressionImpl(streamExpr, context, isBoundaryExpression)
    }
  }

  /**
   * Traverses into a [[StreamExpression]] and returns the resulting [[TraverseNode]] object.
   *
   * @param streamExpr           The [[StreamExpression]] to traverse.
   * @param context              The traversal context.
   * @param isBoundaryExpression Returns true if an expression should not be included in the traversal.
   * @return The [[TraverseNode]] containing the results of the traversal.
   */
  private def traverseStreamExpressionImpl(streamExpr: StreamExpression,
                                           context: Context,
                                           isBoundaryExpression: StreamExpression => Boolean): Option[TraverseNode] = {
    // If this is a boundary expression then short-circuit.
    if (isBoundaryExpression(streamExpr)) {
      None
    }

    val inputNodes = this.traverseInputs(streamExpr, context, isBoundaryExpression)

    val thisUserNode = this.createNode(streamExpr, context)
    val thisNode = TraverseNode(streamExpr, thisUserNode, context)

    this.nodes = this.nodes + (streamExpr.nodeId -> thisNode)

    // If this is a Map or FlatMap of a grouped stream then we will have a node from inside the map function that
    // should be a child of this node.  We also don't want to connect the dependencies of this node to this node,
    // instead we want to connect them to the first node in the map function.  The Map or FlatMap will be the last
    // node in the chain.
    val functionNode = streamExpr match {
      case mapExpr@StreamMap(input, mapFunction) if input.tpe.isInstanceOf[GroupedStreamTypeDescriptor] =>
        assert(inputNodes.length == 1)
        val functionContext = this.getFunctionContext(inputNodes.head, mapExpr, mapFunction, context)
        this.traverse(mapFunction, functionContext, isBoundaryExpression)

      case flatMapExpr@FlatMap(input, flatMapFunction) if input.tpe.isInstanceOf[GroupedStreamTypeDescriptor] =>
        assert(inputNodes.length == 1)
        val functionContext = this.getFunctionContext(inputNodes.head, flatMapExpr, flatMapFunction, context)
        this.traverse(flatMapFunction, functionContext, isBoundaryExpression)

      case _ =>
        inputNodes.foreach(input => this.registerDependency(thisNode.node, input.node))
        None
    }

    // If a node was generated by a map function then it is a child of this node - essentially this means that this node
    // depends on its own map function.
    functionNode match {
      case Some(childNode) =>
        this.registerDependency(thisNode.node, childNode.node)

      case None =>
        ()
    }

    Some(thisNode)
  }

  /**
   * Traverse into the inputs of an expression.
   *
   * @param expr    An expression to traverse the inputs of.
   * @param context The traversal context.
   * @return A list of [[TraverseNode]] objects, one for each of the stream inputs to the expression.
   */
  private def traverseInputs(expr: Tree,
                             context: Context,
                             isBoundaryExpression: StreamExpression => Boolean): List[TraverseNode] = {
    expr match {
      case SingleInputStreamExpression(input) =>
        this.traverse(input, context, isBoundaryExpression).toList

      case TwoInputStreamExpression(input1, input2) =>
        this.traverse(input1, context, isBoundaryExpression).toList ++
          this.traverse(input2, context, isBoundaryExpression).toList

      case _ =>
        List()
    }
  }

  /**
   * Gets a [[Context]] where the arguments of a function that refer to streams are included as terms.
   *
   * @param inputNode   The node corresponding to the stream input of the function.
   * @param contextExpr The stream expression inside which the function is defined.
   * @param function    A function definition.
   * @param context     The current context.
   * @return A new [[Context]] with added terms for the function arguments that refer to the specified stream nodes.
   */
  private def getFunctionContext(inputNode: TraverseNode,
                                 contextExpr: SingleInputStreamExpression,
                                 function: FunctionDef,
                                 context: Context): Context = {
    // If the type of the input stream is a grouped stream then the function operates on streams rather than scalars.
    // The last argument to map functions is the input stream, so we need to add the input node as a named term to the
    // context.
    contextExpr.source.tpe match {
      case GroupedStreamTypeDescriptor(keyType, _) =>
        context
          .withNodeTerm(function.arguments.last.name, inputNode)
          .addToContextKeyType(keyType)
          .withContextStream(inputNode.expr)

      case _ =>
        context
    }
  }

  /**
   * A traversal context which contains the current state of the traversal.
   *
   * @param nodeTerms      Term names and their corresponding [[TraverseNode]] objects.
   *                       When traversing into a function definition, if any function arguments are streams they will be
   *                       referenced here.
   * @param contextStream  A stream that forms the context of the current traversal.
   *                       If traversing into a function definition, the stream expression that contains the definition
   *                       will become the context stream.
   * @param contextKeyType The type of the record key imposed by the context.
   *                       If traversing into a function definition and the context stream is a grouping, this is the
   *                       grouping key. In the case of nested groupings this will be the combined keys of the nested
   *                       groupings.
   */
  case class Context(nodeTerms: Map[String, TraverseNode],
                     contextStream: Option[StreamExpression],
                     contextKeyType: Option[TypeDescriptor[_]]) {
    def withNodeTerm(name: String, node: TraverseNode): Context = {
      Context(this.nodeTerms + (name -> node), this.contextStream, this.contextKeyType)
    }

    def withContextStream(stream: StreamExpression): Context = {
      Context(this.nodeTerms, Some(stream), this.contextKeyType)
    }

    def addToContextKeyType(additionalKeyType: TypeDescriptor[_]): Context = {
      val combinedKeyType =
        this.contextKeyType match {
          case None =>
            TypeDescriptor.createTuple[Product](List(additionalKeyType))

          case Some(tupleType: TupleTypeDescriptor[_]) =>
            TypeDescriptor.augmentTuple(tupleType, additionalKeyType)

          case _ =>
            throw new IllegalArgumentException("Key types must be tuples.")
        }

      Context(this.nodeTerms, this.contextStream, Some(combinedKeyType))
    }
  }

  case class TraverseNode(expr: StreamExpression, node: T, outputContext: Context) {
    override def toString: String = s"TraverseNode(${this.expr.nodeId})"
  }

}
