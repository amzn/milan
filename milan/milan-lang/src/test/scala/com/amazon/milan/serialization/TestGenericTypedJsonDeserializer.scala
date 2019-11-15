package com.amazon.milan.serialization

import com.amazon.milan.typeutil.{TypeDescriptor, _}
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.junit.Assert._
import org.junit.Test


object TestGenericTypedJsonDeserializer {

  @JsonSerialize(using = classOf[Serializer])
  @JsonDeserialize(using = classOf[Deserializer])
  trait BaseClass[T] extends GenericTypeInfoProvider with SetGenericTypeInfo

  // Classes cannot have implicit parameters and still be deserializable by jackson, because any constructors will have a
  // second argument list - the implicit parameters - and jackson doesn't know how to handle that.
  @JsonSerialize
  @JsonDeserialize
  class DerivedClass[T](typeDesc: TypeDescriptor[T]) extends BaseClass[T] {
    @JsonCreator
    def this() {
      this(null)
    }

    private var genericArguments: List[TypeDescriptor[_]] = if (typeDesc == null) List() else List(typeDesc)

    override def getGenericArguments: List[TypeDescriptor[_]] = this.genericArguments

    override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = this.genericArguments = genericArgs

    override def equals(obj: Any): Boolean = this.genericArguments.equals(obj.asInstanceOf[DerivedClass[T]].genericArguments)
  }

  class Container(var x: BaseClass[_]) {
    override def equals(obj: Any): Boolean = this.x.equals(obj.asInstanceOf[Container].x)
  }

  class Serializer extends GenericTypedJsonSerializer[BaseClass[_]]

  class Deserializer extends GenericTypedJsonDeserializer[BaseClass[_]](name => s"com.amazon.milan.serialization.TestGenericTypedJsonDeserializer$$$name")

  case class GenericType[A, B, C](a: A, b: B, c: C)

}

import com.amazon.milan.serialization.TestGenericTypedJsonDeserializer._


@Test
class TestGenericTypedJsonDeserializer {
  @Test
  def test_GenericTypedJsonDeserializer_WithGenericTypeAsTypeParameter(): Unit = {
    val original = new Container(new DerivedClass[GenericType[Int, Long, Double]](createTypeDescriptor[GenericType[Int, Long, Double]]))
    val json = ScalaObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(original)
    val copy = ScalaObjectMapper.readValue(json, classOf[Container])
    assertEquals(original, copy)
  }
}
