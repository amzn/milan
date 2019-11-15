package com.amazon.milan.program

import com.amazon.milan.program.internal.TreeMacros
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.language.experimental.macros


object Tree {
  private val jsonMapper = new ScalaObjectMapper()

  /**
   * Get the Milan expression tree for a scala expression.
   *
   * @param expr A scala expression.
   * @tparam T The type of the scala expression.
   * @return The Milan expression tree corresponding to the scala expression.
   */
  def fromExpression[T](expr: T): Tree = macro TreeMacros.fromExpression

  /**
   * Get the Milan [[FunctionDef]] expression tree for a scala anonymous function.
   *
   * @param expr A scala anonymous function.
   * @return The Milan expression tree corresponding to the scala expression.
   */
  def fromFunction[TIn, TOut](expr: TIn => TOut): FunctionDef = macro TreeMacros.fromFunction

  /**
   * Get the Milan [[FunctionDef]] expression tree for a scala anonymous function.
   *
   * @param expr A scala anonymous function.
   * @return The Milan expression tree corresponding to the scala expression.
   */
  def fromFunction[T1, T2, TOut](expr: (T1, T2) => TOut): FunctionDef = macro TreeMacros.fromFunction

  /**
   * Gets an escaped version of a string.
   */
  private def escape(str: String): String = {
    jsonMapper.writeValueAsString(str)
  }

  /**
   * Gets the [[GraphNodeExpression]] nodes in a tree.
   *
   * @param tree An expression tree.
   * @return A sequence of all of the [[GraphNodeExpression]] nodes in the tree.
   */
  def getStreamExpressions(tree: Tree): Iterable[StreamExpression] = {
    tree match {
      case s: StreamExpression =>
        Seq(s)

      case _ =>
        tree.getChildren.flatMap(this.getStreamExpressions)
    }
  }

  /**
   * Gets a copy of an expression tree with any child [[GraphNodeExpression]] nodes replaced with [[Ref]]
   * nodes.
   *
   * @param tree An expression tree.
   * @return A copy of the expression tree with graph nodes replaced with references.
   */
  def replaceChildStreamsWithReferences(tree: Tree): Tree = {
    val newChildren = tree.getChildren.map(this.replaceStreamsWithReferences).toList
    tree.replaceChildren(newChildren)
  }

  /**
   * Replaces any [[Ref]] nodes in an expression tree with actual nodes from a collection of nodes.
   *
   * @param tree  The tree to replace the references in.
   * @param nodes The actual nodes to use as replacements for the references, keyed by node ID.
   * @return A copy of the input tree with any references replaced with actual nodes.
   */
  def replaceRefsWithActual(tree: Tree,
                            nodes: Map[String, GraphNodeExpression]): Tree = {
    tree match {
      case Ref(nodeId) =>
        val node = nodes(nodeId)
        this.replaceRefsWithActual(node, nodes)

      case _ =>
        val newChildren = tree.getChildren.map(child => this.replaceRefsWithActual(child, nodes)).toList
        tree.replaceChildren(newChildren)
    }
  }


  /**
   * Gets a copy of an expression tree with [[GraphNodeExpression]] nodes replaced with [[Ref]] nodes.
   *
   * @param tree An expression tree.
   * @return A copy of the expression tree with graph nodes replaced with references.
   */
  private def replaceStreamsWithReferences(tree: Tree): Tree = {
    tree match {
      case s: StreamExpression =>
        Ref(s.nodeId)

      case _ =>
        val newChildren = tree.getChildren.map(this.replaceStreamsWithReferences).toList
        tree.replaceChildren(newChildren)
    }
  }
}


/**
 * Base trait for all Milan expression trees.
 */
@JsonSerialize(using = classOf[TreeSerializer])
@JsonDeserialize(using = classOf[TreeDeserializer])
abstract class Tree extends Serializable {
  /**
   * The type of the tree.
   * This can be null for trees that have not been fully typechecked.
   */
  var tpe: TypeDescriptor[_] = _

  /**
   * The expression type.
   */
  val expressionType: String = getClass.getSimpleName

  def getChildren: Iterable[Tree] = List()

  def replaceChildren(children: List[Tree]): Tree = this

  override def toString: String = {
    val cls = getClass

    val fieldStrings =
      cls.getConstructors.head.getParameters.map(p => {
        val fieldName = p.getName
        cls.getMethods.find(_.getName == fieldName) match {
          case Some(getter) =>
            this.valueToString(getter.invoke(this))

          case None if p.getType == classOf[TypeDescriptor[_]] =>
            // Private val constructor parameters of type TypeDescriptor are always equivalent to the tpe value.
            cls.getMethod("tpe").invoke(this).asInstanceOf[TypeDescriptor[_]].toString
        }
      })

    fieldStrings.mkString(getClass.getSimpleName + "(", ", ", ")")
  }

  /**
   * Converts a value to a string that can be included in an expression string.
   *
   * @param value A value.
   * @return A string representation of the value.
   */
  private def valueToString(value: Any): String = value match {
    case t: Tree => t.toString
    case s: String => Tree.escape(s)
    case tr: TypeDescriptor[_] => tr.toString
    case l: List[_] => l.map(valueToString).mkString("List(", ", ", ")")
    case o => o.toString
  }
}


/**
 * Custom serializer for [[Tree]] objects that writes the expression tree as a string.
 */
class TreeSerializer extends JsonSerializer[Tree] {
  override def serialize(t: Tree, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit = {
    jsonGenerator.writeString(t.toString)
  }
}


/**
 * Custom deserializer for [[Tree]] objects that parses an expression tree string.
 */
class TreeDeserializer extends JsonDeserializer[Tree] {
  private val logger = Logger(LoggerFactory.getLogger(getClass))

  override def deserialize(parser: JsonParser, context: DeserializationContext): Tree = {
    val treeString = parser.getValueAsString
    logger.debug(s"Deserializing tree '$treeString'.")
    TreeParser.parse[Tree](treeString)
  }
}
