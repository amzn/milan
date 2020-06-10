package com.amazon.milan.flink.generator

import com.amazon.milan.program.{FunctionDef, SelectTerm, StreamExpression, Tree, TreeOperations, Tuple, TupleElement, TypeChecker, ValueDef}
import com.amazon.milan.typeutil.{TupleTypeDescriptor, TypeDescriptor}

object KeyExtractorUtils {
  /**
   * Combines two key extractor functions into a single function that returns a key that contains the results of both
   * extractors.
   *
   * @param extractorA A key extractor function definition.
   * @param extractorB A key extractor function definition.
   * @return
   */
  def combineKeyExtractors(extractorA: FunctionDef, extractorB: FunctionDef): FunctionDef = {
    // Both extractors must take a single argument.
    if (extractorA.arguments.length != 1 || extractorB.arguments.length != 1) {
      throw new IllegalArgumentException(s"Extractor functions must take exactly one argument.")
    }

    val arg = extractorA.arguments.head

    // Rename the argument in the second tree so that we can use the function body in a new function.
    val extractorBodyB = TreeOperations.renameArgumentInTree(extractorB.body, extractorB.arguments.head.name, arg.name)

    // Each key extractor may return a tuple, or a single value.
    // The output of the combined extractor will be a tuple, so we need to get the expressions for each of those
    // combined tuple elements.
    val combinedTupleElements = this.getTupleElements(extractorA.body) ++ this.getTupleElements(extractorBodyB)

    // Create a new function that produces a tuple of the combined elements.
    val combinedKeyExtractor = FunctionDef(List(arg), Tuple(combinedTupleElements))

    // TypeCheck the combined expression, which will fail if the input expressions are missing type information.
    TypeChecker.typeCheck(combinedKeyExtractor)

    combinedKeyExtractor
  }

  /**
   * Gets a [[FunctionDef]] that defines a function that converts record wrapper key types to the key type expected
   * by a stream expression.
   *
   * @param inputStream The input stream to the expression.
   * @param expr        A stream expression.
   * @return A [[FunctionDef]] that converts the record keys from the input stream to the correct key type for functions
   *         used in the expression.
   */
  def getRecordKeyToKeyFunction(inputStream: GeneratedStream, expr: StreamExpression): FunctionDef = {
    val functionDef = FunctionDef(List(ValueDef("recordKey", inputStream.keyType)), TupleElement(SelectTerm("recordKey"), 0))
    TypeChecker.typeCheck(functionDef)
    functionDef
  }

  def getWindowKeyType(recordKeyType: TypeDescriptor[_]): TypeDescriptor[_] = {
    recordKeyType match {
      case TupleTypeDescriptor(genericArgs) =>
        genericArgs.head

      case _ =>
        throw new IllegalArgumentException(s"Record key types must be tuples.")
    }
  }

  /**
   * Gets the [[TypeDescriptor]] for the type of key that combines information from a key and an additional key element.
   *
   * @param keyType         A key type, which must be a tuple.
   * @param appendedKeyType The type to add to the key.
   * @return A [[TypeDescriptor]] for the key containing the combined types.
   */
  def combineKeyTypes(keyType: TypeDescriptor[_],
                      appendedKeyType: TypeDescriptor[_]): TypeDescriptor[_] = {
    keyType match {
      case keyTupleType: TupleTypeDescriptor[_] =>
        TypeDescriptor.augmentTuple(keyTupleType, appendedKeyType)

      case _ =>
        throw new IllegalArgumentException("Key types must be tuples.")
    }
  }

  /**
   * Gets the [[TypeDescriptor]] for the type of key that combines information from a key and an additional key element.
   *
   * @param inputKeyType      A key type, which must be a tuple.
   * @param addedKeyType      The type to add to the key.
   * @param isInputContextual Specifies whether the input stream is contextual.
   *                          If it is, the new key type is appended to it.
   *                          If it is not contextual, the new key type replaces the last element of the input key tuple.
   * @return A [[TypeDescriptor]] for the key containing the combined types.
   */
  def addToKey(inputKeyType: TypeDescriptor[_],
               addedKeyType: TypeDescriptor[_],
               isInputContextual: Boolean): TypeDescriptor[_] = {
    if (isInputContextual) {
      this.combineKeyTypes(inputKeyType, addedKeyType)
    }
    else {
      this.combineKeyTypes(this.removeLastKeyElement(inputKeyType), addedKeyType)
    }
  }

  def removeLastKeyElement(keyType: TypeDescriptor[_]): TypeDescriptor[_] = {
    keyType match {
      case keyTupleType: TupleTypeDescriptor[_] =>
        TypeDescriptor.createTuple[Product](keyTupleType.genericArguments.take(keyTupleType.genericArguments.length - 1))

      case _ =>
        throw new IllegalArgumentException("Key types must be tuples.")
    }
  }

  private def getTupleElements(tree: Tree): List[Tree] = {
    tree match {
      case Tuple(elements) => elements
      case _ => List(tree)
    }
  }

}
