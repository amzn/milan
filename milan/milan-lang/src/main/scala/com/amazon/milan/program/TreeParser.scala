package com.amazon.milan.program

import com.amazon.milan.typeutil.{TypeDescriptor, TypeFactory, types}

import scala.collection.mutable


class TreeParseException(tree: String, cause: Throwable = null) extends Exception(s"Error parsing tree: $tree", cause)


class TreeParser(typeFactory: TypeFactory) {
  def this() {
    this(new TypeFactory())
  }

  /**
   * Parses an expression tree string into a Milan expression tree object.
   *
   * @param treeString An expression tree string.
   * @tparam T The expected output tree type.
   * @return The parsed tree.
   */
  def parse[T <: Tree](treeString: String): T = {
    val root = parseTree(treeString)

    try {
      convertNode[T](root)
    }
    catch {
      case ex: Throwable =>
        throw new TreeParseException(treeString, ex)
    }
  }

  /**
   * Converts a [[ParseNode]] into its Milan expression tree.
   *
   * @param node The [[ParseNode]] to convert.
   * @tparam T The expected output tree type.
   * @return The converted Milan tree objects.
   */
  private def convertNode[T](node: ParseNode): T = {
    val asParentNode = node.asInstanceOf[ParentNode]
    val name = asParentNode.name
    val children = asParentNode.children

    val converted =
      name match {
        case "And" => And(convertNode[Tree](children(0)), convertNode[Tree](children(1)))
        case "ApplyFunction" => ApplyFunction(convertNode[FunctionReference](children(0)), convertList[Tree](children(1)), convertTypeDescriptor(children(2)))
        case "ConstantValue" => convertConstant(children)
        case "ConvertType" => ConvertType(convertNode[Tree](children(0)), convertTypeDescriptor(children(1)))
        case "CreateInstance" => CreateInstance(convertTypeDescriptor(children(0)), convertList[Tree](children(1)))
        case "Duration" => Duration(children(0).asValueNode.valueString.toLong)
        case "Equals" => Equals(convertNode[Tree](children(0)), convertNode[Tree](children(1)))
        case "FieldDefinition" => FieldDefinition(getString(children(0)), convertNode[FunctionDef](children(1)))
        case "FunctionDef" => FunctionDef(convertStringList(children(0)), convertNode[Tree](children(1)))
        case "FunctionReference" => FunctionReference(getString(children(0)), getString(children(1)))
        case "GreaterThan" => GreaterThan(convertNode[Tree](children(0)), convertNode[Tree](children(1)))
        case "IfThenElse" => IfThenElse(convertNode[Tree](children(0)), convertNode[Tree](children(1)), convertNode[Tree](children(2)))
        case "IsNull" => IsNull(convertNode[Tree](children(0)))
        case "LessThan" => LessThan(convertNode[Tree](children(0)), convertNode[Tree](children(1)))
        case "Minus" => Minus(convertNode[Tree](children(0)), convertNode[Tree](children(1)))
        case "Not" => Not(convertNode[Tree](children(0)))
        case "Plus" => Plus(convertNode[Tree](children(0)), convertNode[Tree](children(1)))
        case "SelectTerm" => SelectTerm(getString(children(0)))
        case "SelectField" => SelectField(convertNode[SelectExpression](children(0)), getString(children(1)))
        case "Tuple" => Tuple(convertList[Tree](children.head))
        case "TypeDescriptor" => convertTypeDescriptor(children(0))
        case "Unpack" => Unpack(convertNode[SelectTerm](children(0)), convertStringList(children(1)), convertNode[Tree](children(2)))

        // Graph node expressions
        case "FullJoin" => FullJoin(convertNode[StreamExpression](children(0)), convertNode[StreamExpression](children(1)), convertNode[FunctionDef](children(2)))
        case "Ref" => Ref(getString(children(0)))
        case "GroupBy" => GroupBy(convertNode[StreamExpression](children(0)), convertNode[FunctionDef](children(1)))
        case "LeftJoin" => LeftJoin(convertNode[StreamExpression](children(0)), convertNode[StreamExpression](children(1)), convertNode[FunctionDef](children(2)))
        case "MapRecord" => MapRecord(convertNode[GraphNodeExpression](children(0)), convertNode[FunctionDef](children(1)))
        case "MapFields" => MapFields(convertNode[GraphNodeExpression](children(0)), convertList[FieldDefinition](children(1)))
        case "Filter" => Filter(convertNode[StreamExpression](children(0)), convertNode[FunctionDef](children(1)))
        case "SlidingWindow" => SlidingWindow(convertNode[StreamExpression](children(0)), convertNode[FunctionDef](children(1)), convertNode[Duration](children(2)), convertNode[Duration](children(3)), convertNode[Duration](children(4)))
        case "TumblingWindow" => TumblingWindow(convertNode[StreamExpression](children(0)), convertNode[FunctionDef](children(1)), convertNode[Duration](children(2)), convertNode[Duration](children(3)))
        case "UniqueBy" => UniqueBy(convertNode[GroupingExpression](children(0)), convertNode[FunctionDef](children(1)))

        // Built-in functions
        case "Sum" => Sum(convertNode[Tree](children.head))
        case "Min" => Min(convertNode[Tree](children.head))
        case "Max" => Max(convertNode[Tree](children.head))
        case "Mean" => Mean(convertNode[Tree](children.head))
        case "First" => First(convertNode[Tree](children.head))
        case "ArgMin" => ArgMin(convertNode[Tuple](children.head))
        case "ArgMax" => ArgMax(convertNode[Tuple](children.head))
      }

    converted.asInstanceOf[T]
  }

  /**
   * Converts a [[ParseNode]] that contains a list of trees into a list of Milan expression trees.
   *
   * @param node The [[ParseNode]] to convert.
   * @tparam T The expected output tree type.
   * @return A list of converted Milan tree objects.
   */
  private def convertList[T <: Tree](node: ParseNode): List[T] = {
    val listNode = node.asInstanceOf[ParentNode]

    if (listNode.name != "List") {
      throw new TreeParseException(listNode.name)
    }
    listNode.children.map(convertNode[T])
  }

  /**
   * Converts a [[ParseNode]] that contains a list of string values into a list of strings.
   *
   * @param node The [[ParseNode]] to convert.
   * @return The converted list of strings.
   */
  private def convertStringList(node: ParseNode): List[String] = {
    val listNode = node.asInstanceOf[ParentNode]

    if (listNode.name != "List") {
      throw new TreeParseException(listNode.name)
    }

    listNode.children.map(getString)
  }

  /**
   * Converts a [[ParseNode]] that contains a constant value into a [[ConstantValue]] node.
   *
   * @param children The children of the parse node containing the constant value.
   *                 The first child should contain the value itself, the second should contain the type descriptor for
   *                 the value type.
   * @return A [[ConstantValue]]
   */
  private def convertConstant(children: List[ParseNode]): ConstantValue = {
    val valueType = convertTypeDescriptor(children(1))
    val value = getValue(children(0), valueType)
    ConstantValue(value, valueType)
  }

  /**
   * Converts a [[ParseNode]] representing a [[TypeDescriptor]] into a [[TypeDescriptor]] object.
   *
   * @param node A parse node.
   * @return A [[TypeDescriptor]] created from the information in the parse node.
   */
  private def convertTypeDescriptor(node: ParseNode): TypeDescriptor[_] = {
    val typeName = this.getString(node.asParentNode.children.head)
    this.typeFactory.getTypeDescriptor[Any](typeName)
  }

  /**
   * Gets the value from a [[ParseNode]] that contains a value.
   *
   * @param node The [[ParseNode]] to convert.
   * @return The value contained in the node.
   */
  private def getValue(node: ParseNode, ty: TypeDescriptor[_]): Any = {
    val valueString = node.asValueNode.valueString

    ty match {
      case types.Double => valueString.toDouble
      case types.Float => valueString.toFloat
      case types.Int => valueString.toInt
      case types.String => valueString
      case types.Boolean => valueString.toBoolean
      case types.Long => valueString.toLong
      case _ => throw new InvalidProgramException(s"Unsupported constant value type '${ty.fullName}'.")
    }
  }

  /**
   * Gets the string value from a [[ParseNode]] that contains a string value.
   *
   * @param node The [[ParseNode]] to convert.
   * @return The value contained in the node.
   */
  private def getString(node: ParseNode): String = {
    node.asValueNode.valueString
  }

  /**
   * A node in the tree produced from parsing an expression string.
   */
  private trait ParseNode {
    def asParentNode: ParentNode = this.asInstanceOf[ParentNode]

    def asValueNode: ValueNode = this.asInstanceOf[ValueNode]
  }

  /**
   * A node that contains a value.
   *
   * @param valueString The string representation of the value in the node.
   */
  private case class ValueNode(valueString: String) extends ParseNode

  /**
   * A node that contains children.
   *
   * @param name     The name of the node.
   * @param children The node's children.
   */
  private case class ParentNode(name: String, children: List[ParseNode]) extends ParseNode

  /**
   * Parses an expression tree string into a tree of [[ParseNode]] objects.
   *
   * @param treeString An expression tree string.
   * @return The [[ParentNode]] node object that is the root of the parsed tree.
   */
  private def parseTree(treeString: String): ParentNode = {
    // The parser is implemented as a state machine that processes one character at a time.
    // It maintains a stack of the nodes that are being processed.
    var stack = List(("root", new mutable.MutableList[ParseNode]))
    val currentValue = new mutable.StringBuilder()

    // These are the possible states of the parser state machine.
    val NODE_NAME = 1
    val CHILD_LIST = 2
    val STRING_VALUE = 3
    val NUMERIC_VALUE = 4
    val ESCAPED_CHAR = 5

    var state = NODE_NAME

    for (c <- treeString) {
      state match {
        case NODE_NAME =>
          c match {
            case '(' =>
              // Open a new ParentNode on the stack and start collecting its children.
              val name = currentValue.toString()
              stack = (name, new mutable.MutableList[ParseNode]) :: stack
              state = CHILD_LIST

            case o =>
              currentValue.append(o)
          }

        case CHILD_LIST =>
          c match {
            case ',' => ()
            case ' ' => ()

            case '"' =>
              currentValue.clear()
              state = STRING_VALUE

            case d if isDigit(d) =>
              currentValue.clear()
              currentValue.append(d)
              state = NUMERIC_VALUE

            case ')' =>
              // Create the ParentNode object that just closed.
              val (name, args) = stack.head
              val node = ParentNode(name, args.toList)

              // Pop this node off the stack.
              stack = stack.tail

              // Now add the new node to its parent's arguments.
              val (_, parentArgs) = stack.head
              parentArgs += node
              state = CHILD_LIST

            case o =>
              // This is the start of a node name.
              currentValue.clear()
              currentValue.append(o)
              state = NODE_NAME
          }

        case STRING_VALUE =>
          c match {
            case '\\' =>
              state = ESCAPED_CHAR

            case '"' =>
              // This is the end of a string value, add it as an argument to the current ParentNode.
              val (_, currentArgs) = stack.head
              currentArgs += ValueNode(currentValue.toString())
              currentValue.clear()
              state = CHILD_LIST

            case o =>
              currentValue.append(o)
          }

        case NUMERIC_VALUE =>
          c match {
            case ',' =>
              val (_, currentArgs) = stack.head
              currentArgs += ValueNode(currentValue.toString())
              currentValue.clear()
              state = CHILD_LIST

            case o if isDigit(o) =>
              currentValue.append(c)

            case '.' if !currentValue.contains('.') =>
              // Only allow a single decimal point.
              currentValue.append(c)

            case ')' =>
              // Add a value node for the number to the current args list.
              val (name, args) = stack.head
              args += ValueNode(currentValue.toString())

              // Create the ParentNode object that just closed.
              val node = ParentNode(name, args.toList)

              // Pop this node off the stack.
              stack = stack.tail

              // Now add the new node to its parent's arguments.
              val (_, parentArgs) = stack.head
              parentArgs += node
              state = CHILD_LIST

            case _ =>
              throw new InvalidProgramException(s"Invalid character '$c' in numeric value, while parsing '$treeString'.")
          }

        case ESCAPED_CHAR =>
          currentValue.append(c)
          state = STRING_VALUE
      }
    }

    val (_, rootChildren) = stack.head
    rootChildren.head.asInstanceOf[ParentNode]
  }

  private def isDigit(c: Char): Boolean = {
    c >= '0' && c <= '9'
  }
}


object TreeParser {
  /**
   * Parses an expression tree string into a Milan expression tree object.
   *
   * @param treeString An expression tree string.
   * @tparam T The expected output tree type.
   * @return The parsed tree.
   */
  def parse[T <: Tree](treeString: String): T = {
    val parser = new TreeParser()
    parser.parse[T](treeString)
  }
}
