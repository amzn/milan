package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.program._
import com.amazon.milan.typeutil.{FieldDescriptor, TypeDescriptor}

import scala.language.existentials


object ContextualTreeTransformer {

  trait TransformationContext {
    def getType(name: String): TypeDescriptor[_]

    def resolve(select: SelectExpression): Tree

    def transform(tree: Tree, transformedChildren: List[Tree]): Tree = {
      tree match {
        case select: SelectExpression => this.resolve(select)
        case _ => tree.replaceChildren(transformedChildren)
      }
    }
  }

  class FunctionDefTransformationContext(args: Map[String, TypeDescriptor[_]]) extends TransformationContext {
    override def getType(name: String): TypeDescriptor[_] = args(name)

    override def resolve(select: SelectExpression): Tree = select
  }

  class UnpackWithFieldNamesTransformationContext(argName: String,
                                                  valueFields: Map[String, FieldDescriptor[_]],
                                                  parent: TransformationContext) extends TransformationContext {
    override def getType(name: String): TypeDescriptor[_] = {
      this.valueFields.get(name) match {
        case Some(field) => field.fieldType
        case _ => this.parent.getType(name)
      }
    }

    override def resolve(select: SelectExpression): Tree = {
      val termName = select.getRootTermName

      this.valueFields.get(termName) match {
        case Some(field) =>
          select.replaceRootTermName(SelectField(SelectTerm(this.argName), field.name))

        case _ =>
          this.parent.resolve(select)
      }
    }

    override def transform(tree: Tree, transformedChildren: List[Tree]): Tree = {
      tree match {
        case _: Unpack =>
          // The last entry in the transformed children is the transformed version of the interior of the unpack expression.
          // Because we have field names available for the stream record being unpacked, we have transformed any
          // SelectTerm expressions that refer to the unpacked values into SelectField expressions that select from the
          // input stream directly. This means we don't need the Unpack expression any more.
          transformedChildren.last

        case _ =>
          super.transform(tree, transformedChildren)
      }
    }
  }

  class UnpackTransformationContext(argName: String,
                                    valueTypes: Map[String, TypeDescriptor[_]],
                                    parent: TransformationContext) extends TransformationContext {
    override def getType(name: String): TypeDescriptor[_] = {
      this.valueTypes.get(name) match {
        case Some(ty) => ty
        case _ => this.parent.getType(name)
      }
    }

    override def resolve(select: SelectExpression): Tree = {
      val termName = select.getRootTermName

      this.valueTypes.get(termName) match {
        case Some(_) =>
          // We can't collapse select statements because we're unpacking a standard tuple object, not a stream record.
          select

        case _ =>
          this.parent.resolve(select)
      }
    }
  }

  /**
   * Transforms a [[FunctionDef]] into one that may be simpler, by taking the input argument types into account.
   *
   * @param functionDef   A function definition.
   * @param argumentTypes The types of the function arguments.
   * @return
   */
  def transform(functionDef: FunctionDef, argumentTypes: List[TypeDescriptor[_]]): FunctionDef = {
    if (functionDef.arguments.length != argumentTypes.length) {
      throw new IllegalArgumentException(s"Number of function arguments (${functionDef.arguments.length} does not match number of argument types supplied (${argumentTypes.length}).")
    }

    val valueTypes = functionDef.arguments.zip(argumentTypes).toMap
    val context = new FunctionDefTransformationContext(valueTypes)
    this.transform(functionDef, context).asInstanceOf[FunctionDef]
  }

  private def transform(tree: Tree, context: TransformationContext): Tree = {
    val treeContext = this.getTransformationContext(tree, context)
    val transformedChildren = tree.getChildren.map(child => this.transform(child, treeContext)).toList

    treeContext.transform(tree, transformedChildren)
  }

  private def getTransformationContext(tree: Tree, parent: TransformationContext): TransformationContext = {
    tree match {
      case Unpack(target, valueNames, _) =>
        val argName = target.termName
        val valueTypes = parent.getType(argName).fields

        if (valueTypes.length != valueNames.length) {
          val valueTypes = valueNames.zip(target.tpe.genericArguments).toMap
          new UnpackTransformationContext(argName, valueTypes, parent)
        }
        else {
          val valueFields = valueNames.zip(valueTypes).toMap
          new UnpackWithFieldNamesTransformationContext(argName, valueFields, parent)
        }

      case _ =>
        parent
    }
  }
}
