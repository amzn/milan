package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala.{CodeBlock, TypeLifter, ValName}
import com.amazon.milan.typeutil.{TypeDescriptor, types}

trait KeyOperationsGenerator {

  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Gets a code snippet the returns the context key portion of a full key.
   */
  def generateGetContextKeyCode(fullKeyVal: ValName, fullKeyType: TypeDescriptor[_], contextKeyType: TypeDescriptor[_]): CodeBlock = {
    if (contextKeyType == types.EmptyTuple) {
      qc"Option.empty"
    }
    else if (fullKeyType == contextKeyType) {
      qc"$fullKeyVal"
    }
    else if (!fullKeyType.isTuple) {
      // If the full key isn't the same as the context key, then the full key must be a tuple, otherwise there's no
      // way to extract anything from it.
      throw new IllegalArgumentException("Context key cannot be part of the full key.")
    }
    else if (!contextKeyType.isTuple) {
      // If the context key is a scalar then get the first value from the full key.
      qc"$fullKeyVal._1"
    }
    else {
      // Both are tuples, so we need to unpack the full key and create a new tuple from the subset that is the context
      // key
      val contextKeyLength = contextKeyType.genericArguments.length
      val fullKeyLength = fullKeyType.genericArguments.length

      if (fullKeyLength < contextKeyLength) {
        throw new IllegalArgumentException(s"Context key has length $contextKeyLength but full key only has length $fullKeyLength.")
      }
      else if (fullKeyLength == contextKeyLength) {
        qc"$fullKeyVal"
      }
      else {
        val fullKeyParts = List.tabulate(fullKeyLength)(i => ValName(s"k${i + 1}"))
        val contextKeyParts = fullKeyParts.take(contextKeyLength)

        qc"""
            |val Tuple$fullKeyLength(..$fullKeyParts) = $fullKeyVal
            |Tuple$contextKeyLength(..$contextKeyParts)
            | """
      }
    }
  }

  /**
   * Gets a code snippet the returns the local key portion of a full key.
   */
  def generateGetLocalKeyCode(fullKeyVal: ValName, fullKeyType: TypeDescriptor[_], keyType: TypeDescriptor[_]): CodeBlock = {
    if (keyType == types.EmptyTuple) {
      qc"Option.empty"
    }
    else if (fullKeyType == keyType) {
      qc"$fullKeyVal"
    }
    else if (!fullKeyType.isTuple) {
      // If the full key isn't the same as the key, then the full key must be a tuple, otherwise there's no
      // way to extract anything from it.
      throw new IllegalArgumentException("Key cannot be part of the full key.")
    }
    else if (keyType.isTuple) {
      val keyLength = keyType.genericArguments.length
      val fullKeyLength = fullKeyType.genericArguments.length

      // If the local key can't be contained in the full key then something is wrong.
      if (fullKeyLength < keyLength) {
        throw new IllegalArgumentException(s"Key has length $keyLength but full key only has length $fullKeyLength.")
      }
      else if (fullKeyLength == keyLength) {
        qc"$fullKeyVal"
      }
      else {
        val fullKeyParts = List.tabulate(fullKeyLength)(i => ValName(s"k${i + 1}"))
        val localKeyParts = fullKeyParts.drop(fullKeyLength - keyLength)

        qc"""
            |val Tuple$fullKeyLength(..$fullKeyParts) = $fullKeyVal
            |Tuple$keyLength(..$localKeyParts)
            | """
      }
    }
    else {
      // The key is a scalar which means it's the last part of the full key.
      val fullKeyLength = fullKeyType.genericArguments.length
      val fullKeyParts = List.tabulate(fullKeyLength)(i => ValName(s"k${i + 1}"))
      val lastKeyPart = fullKeyParts.last

      qc"""
          |val Tuple$fullKeyLength(..$fullKeyParts) = $fullKeyVal
          |$lastKeyPart
          |"""
    }
  }
}
