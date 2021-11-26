package com.amazon.milan.typeutil


import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


@JsonSerialize
@JsonDeserialize
class BasicTypeDescriptor[T](val typeName: String) extends TypeDescriptor[T] {
  val genericArguments = List()
  val fields = List()
}


final class NumericTypeDescriptor[T](typeName: String) extends BasicTypeDescriptor[T](typeName)


@JsonSerialize
@JsonDeserialize
final class TupleTypeDescriptor[T](val typeName: String,
                                   val genericArguments: List[TypeDescriptor[_]],
                                   val fields: List[FieldDescriptor[_]]) extends TypeDescriptor[T] {

  def this(fields: List[FieldDescriptor[_]]) {
    this(if (fields.isEmpty) "Product" else s"Tuple${fields.length}", fields.map(_.fieldType), fields)
  }
}

object TupleTypeDescriptor {
  def unapply(arg: TupleTypeDescriptor[_]): Option[List[TypeDescriptor[_]]] = Some(arg.genericArguments)
}


/**
 * [[TypeDescriptor]] implementation to use for constructing custom type descriptors.
 *
 * @param typeName         The name of the type, not including generic arguments.
 * @param genericArguments The generic arguments of the type.
 * @param fields           The fieldsof the type.
 * @tparam T The type being described.
 */
@JsonSerialize
@JsonDeserialize
final class ObjectTypeDescriptor[T](val typeName: String,
                                    val genericArguments: List[TypeDescriptor[_]],
                                    val fields: List[FieldDescriptor[_]]) extends TypeDescriptor[T] {
  assert(!typeName.matches("^Tuple[0-9]+$"))
}


/**
 * [[TypeDescriptor]] implementation used for generated type descriptors, e.g. by using `TypeDescriptor.of`.
 */
@JsonSerialize
@JsonDeserialize
final class GeneratedTypeDescriptor[T](val typeName: String,
                                       val genericArguments: List[TypeDescriptor[_]],
                                       val fields: List[FieldDescriptor[_]]) extends TypeDescriptor[T] {
  assert(!typeName.matches("^Tuple[0-9]+$"))
}


@JsonSerialize
@JsonDeserialize
final class CollectionTypeDescriptor[T](val typeName: String,
                                        val genericArguments: List[TypeDescriptor[_]]) extends TypeDescriptor[T] {
  override val fields: List[FieldDescriptor[_]] = List()
}


trait StreamTypeDescriptor extends TypeDescriptor[Any] {
  def recordType: TypeDescriptor[_]
}

@JsonSerialize
@JsonDeserialize
final class DataStreamTypeDescriptor(val recordType: TypeDescriptor[_]) extends StreamTypeDescriptor {
  if (recordType == null) {
    throw new IllegalArgumentException()
  }

  override val typeName: String = "Stream"

  override val fields: List[FieldDescriptor[_]] = {
    // Although the record type may have fields, a stream itself does not have any fields.
    List()
  }

  override val genericArguments: List[TypeDescriptor[_]] = List(this.recordType)
}

object DataStreamTypeDescriptor {
  def unapply(arg: DataStreamTypeDescriptor): Option[TypeDescriptor[_]] = Some(arg.recordType)
}


@JsonSerialize
@JsonDeserialize
final class JoinedStreamsTypeDescriptor(val leftRecordType: TypeDescriptor[_],
                                        val rightRecordType: TypeDescriptor[_])
  extends StreamTypeDescriptor {

  override def recordType: TypeDescriptor[_] = new TupleTypeDescriptor[Any]("Tuple2", List(leftRecordType, rightRecordType), List())

  override val typeName: String = "JoinedStreams"

  override val fields: List[FieldDescriptor[_]] = List(
    FieldDescriptor("left", this.leftRecordType),
    FieldDescriptor("right", this.rightRecordType))

  override val genericArguments: List[TypeDescriptor[_]] = List(this.leftRecordType, this.rightRecordType)
}

object JoinedStreamsTypeDescriptor {
  def unapply(arg: JoinedStreamsTypeDescriptor): Option[(TypeDescriptor[_], TypeDescriptor[_])] = Some((arg.leftRecordType, arg.rightRecordType))
}


@JsonSerialize
@JsonDeserialize
final class GroupedStreamTypeDescriptor(val keyType: TypeDescriptor[_],
                                        val recordType: TypeDescriptor[_]) extends StreamTypeDescriptor {
  override val typeName: String = "GroupedStream"

  override val fields: List[FieldDescriptor[_]] = List()

  override val genericArguments: List[TypeDescriptor[_]] = List(this.keyType, this.recordType)
}

object GroupedStreamTypeDescriptor {
  def unapply(arg: GroupedStreamTypeDescriptor): Option[(TypeDescriptor[_], TypeDescriptor[_])] =
    Some((arg.keyType, arg.recordType))
}
