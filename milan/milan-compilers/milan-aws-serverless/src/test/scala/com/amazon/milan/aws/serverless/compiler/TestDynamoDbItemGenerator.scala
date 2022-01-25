package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.compiler.scala.testing.KeyValueRecord
import com.amazon.milan.compiler.scala.{ClassName, DefaultTypeEmitter, MilanScalaCompilerTypeDescriptorExtensions, RuntimeEvaluator, TypeLifter, ValName}
import com.amazon.milan.typeutil.TypeDescriptor
import org.junit.Assert._
import org.junit.Test
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util

object TestDynamoDbItemGenerator {
  trait Generator[T] {
    def generate(value: T): util.HashMap[String, Any]
  }

  case class ListRecord(intField: Int, stringField: String, listField: List[Int])

  case class NestedRecord(recordId: String, keyValueField: KeyValueRecord)

  case class ListOfObjectsRecord(listField: List[KeyValueRecord])

  case class ThreeLevelNestedRecord(recordId: String, nestedRecord: NestedRecord)
}


/**
 * We need a special TypeLifter that converts AttributeValue to Any.
 * If AttributeValue appears in any generated code, we can't evaluate it at runtime for some reason.
 */
class AvoidAttributeValueTypeLifter extends TypeLifter(new DefaultTypeEmitter) {
  override protected def toCanonicalName(typeName: String): String = {
    if (typeName == "software.amazon.awssdk.services.dynamodb.model.AttributeValue") {
      "Any"
    }
    else {
      super.toCanonicalName(typeName)
    }
  }
}

import com.amazon.milan.aws.serverless.compiler.TestDynamoDbItemGenerator._


@Test
class TestDynamoDbItemGenerator {
  val typeLifter = new AvoidAttributeValueTypeLifter

  import typeLifter._

  @Test
  def test_DynamoDbItemGenerator_GenerateCreateDynamoDbItemMethod_OfKeyValueRecord_ReturnsWorkingCode(): Unit = {
    val ty = TypeDescriptor.of[KeyValueRecord]
    val target = this.generateGenerator(ty)

    val record = KeyValueRecord(1, 2)
    val output = target.generate(record)

    assertEquals(s(record.recordId), output.get("recordId"))
    assertEquals(n(1), output.get("key"))
    assertEquals(n(2), output.get("value"))
  }

  @Test
  def test_DynamoDbItemGenerator_GenerateCreateDynamoDbItemMethod_OfListRecord_ReturnsWorkingCode(): Unit = {
    val ty = TypeDescriptor.of[ListRecord]
    val target = this.generateGenerator(ty)

    val record = ListRecord(1, "s", List(1, 2, 3))
    val output = target.generate(record)

    assertEquals(n(1), output.get("intField"))
    assertEquals(s("s"), output.get("stringField"))
    assertEquals(l(n(1), n(2), n(3)), output.get("listField"))
  }

  @Test
  def test_DynamoDbItemGenerator_GenerateCreateDynamoDbItemMethod_OfNestedRecord_ReturnsWorkingCode(): Unit = {
    val ty = TypeDescriptor.of[NestedRecord]
    val target = this.generateGenerator(ty)

    val record = NestedRecord("id", KeyValueRecord(1, 2))
    val output = target.generate(record)

    assertEquals(s("id"), output.get("recordId"))
    assertEquals(
      m(
        "recordId" -> s(record.keyValueField.recordId),
        "key" -> n(1),
        "value" -> n(2)
      ),
      output.get("keyValueField")
    )
  }

  def generateGenerator[T](recordType: TypeDescriptor[T]): Generator[T] = {
    val recordVal = ValName("record")
    val mapVal = ValName("map")

    val generator = new DynamoDbItemGenerator(this.typeLifter)
    val addMapValuesCode = generator.generateAddMapValuesFromObject(recordVal, mapVal, recordType)

    val code =
      qc"""
        |class TestClass extends ${nameOf[Generator[Any]]}[${recordType.toTerm}] {
        |  def generate($recordVal: ${recordType.toTerm}): ${nameOf[util.HashMap[Any, Any]]}[String, Any] = {
        |    val $mapVal = new ${nameOf[util.HashMap[Any, Any]]}[String, Any]()
        |    ${addMapValuesCode.indentTail(2)}
        |    $mapVal
        |  }
        |}
        |
        |new TestClass
        |"""

    val eval = new RuntimeEvaluator(classOf[AttributeValue].getClassLoader)
    eval.eval[Generator[T]](code.value)
  }

  private def s(value: String): AttributeValue =
    AttributeValue.builder().s(value).build()

  private def n(value: Int): AttributeValue =
    AttributeValue.builder().n(value.toString).build()

  private def l(items: AttributeValue*): AttributeValue =
    AttributeValue.builder().l(items: _*).build()

  private def m(items: (String, AttributeValue)*): AttributeValue = {
    val map = new util.HashMap[String, AttributeValue]()
    items.foreach {
      case (k, v) => map.put(k, v)
    }
    AttributeValue.builder().m(map).build()
  }
}
