package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.scala.TreeTransformer
import com.amazon.milan.program.{FunctionDef, SelectField, SelectTerm, Tree, TupleElement, Unpack}
import com.amazon.milan.typeutil.{FieldDescriptor, TypeDescriptor}


object ContextualTreeTransformer extends TreeTransformer {

  trait TransformationContext {
    def getType(name: String): TypeDescriptor[_]

    def resolve(select: SelectTerm): Tree

    def transform(tree: Tree, transformedChildren: List[Tree]): Tree = {
      tree match {
        case select: SelectTerm => this.resolve(select)
        case _ => tree.replaceChildren(transformedChildren)
      }
    }
  }

  class FunctionDefTransformationContext(args: Map[String, TypeDescriptor[_]]) extends TransformationContext {
    override def getType(name: String): TypeDescriptor[_] = args(name)

    override def resolve(select: SelectTerm): Tree = select
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

    override def resolve(select: SelectTerm): Tree = {
      this.valueFields.get(select.termName) match {
        case Some(field) =>
          SelectField(SelectTerm(this.argName), field.name)

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
                                    values: List[(String, TypeDescriptor[_])],
                                    parent: TransformationContext) extends TransformationContext {
    override def getType(name: String): TypeDescriptor[_] = {
      this.values.find { case (valueName, _) => valueName == name } match {
        case Some((_, ty)) => ty
        case _ => this.parent.getType(name)
      }
    }

    override def resolve(select: SelectTerm): Tree = {
      val termName = select.termName

      this.values.indexWhere { case (valueName, _) => valueName == termName } match {
        case -1 =>
          this.parent.resolve(select)

        case i =>
          TupleElement(SelectTerm(this.argName), i)
      }
    }

    override def transform(tree: Tree, transformedChildren: List[Tree]): Tree = {
      tree match {
        case _: Unpack =>
          transformedChildren.last

        case _ =>
          super.transform(tree, transformedChildren)
      }
    }
  }

  /**
   * Transforms a [[FunctionDef]] into one that may be simpler, by taking the input argument types into account.
   *
   * @param functionDef A function definition.
   * @return
   */
  def transform(functionDef: FunctionDef): FunctionDef = {
    validateFunctionArgsHaveTypes(functionDef)

    val valueTypes = functionDef.arguments.map(arg => arg.name -> arg.tpe).toMap[String, TypeDescriptor[_]]
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
      case Unpack(SelectTerm(argName), valueNames, _) =>
        val argType = parent.getType(argName)
        val valueTypes = argType.fields

        if (valueTypes.length != valueNames.length) {
          val valueTypes = valueNames.zip(argType.genericArguments)
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
