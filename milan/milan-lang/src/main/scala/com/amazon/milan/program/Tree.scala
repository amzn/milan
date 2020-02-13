package com.amazon.milan.program

import com.amazon.milan.program.internal.TreeMacros
import com.amazon.milan.serialization.{ScalaObjectMapper, TypeInfoProvider, TypedJsonDeserializer, TypedJsonSerializer}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, TypeDescriptor}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

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
   * Gets the [[StreamExpression]] nodes in a tree where type argument is a record type and not a stream.
   *
   * @param tree An expression tree.
   * @return A sequence of all of the [[StreamExpression]] nodes in the tree.
   */
  def getDataStreams(tree: Tree): Iterable[StreamExpression] = {
    tree match {
      case s: StreamExpression if this.isDataStream(s) =>
        Seq(s)

      case _ =>
        tree.getChildren.flatMap(this.getDataStreams)
    }
  }

  def isDataStream(tree: Tree): Boolean = {
    tree.tpe.isInstanceOf[DataStreamTypeDescriptor]
  }

  /**
   * Gets a copy of an expression tree with any child [[StreamExpression]] nodes replaced with [[Ref]]
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
                            nodes: Map[String, StreamExpression]): Tree = {
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
   * Gets a copy of an expression tree with [[StreamExpression]] nodes replaced with [[Ref]] nodes.
   *
   * @param tree An expression tree.
   * @return A copy of the expression tree with graph nodes replaced with references.
   */
  private def replaceStreamsWithReferences(tree: Tree): Tree = {
    tree match {
      case s: StreamExpression if this.isDataStream(s) =>
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
abstract class Tree extends Serializable with TypeInfoProvider {
  /**
   * The type of the tree.
   * This can be null for trees that have not been fully typechecked.
   */
  var tpe: TypeDescriptor[_] = _

  /**
   * The expression type.
   */
  @JsonIgnore
  val expressionType: String = getClass.getSimpleName

  @JsonIgnore
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
            if (this.tpe == null) {
              "null"
            }
            else {
              this.tpe.toString
            }
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
 * Custom serializer for [[Tree]] objects.
 */
class TreeSerializer extends TypedJsonSerializer[Tree]


/**
 * Custom deserializer for [[Tree]] objects.
 */
class TreeDeserializer extends TypedJsonDeserializer[Tree]("com.amazon.milan.program")
