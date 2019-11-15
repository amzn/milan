package com.amazon.milan.typeutil


import com.fasterxml.jackson.annotation.JsonIgnore
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
    this(s"Tuple${fields.length}", fields.map(_.fieldType), fields)
  }

  @JsonIgnore
  override def verboseName: String = {
    if (this.fields.nonEmpty) {
      typeName + fields.map(f => s"${f.name}: ${f.fieldType.verboseName}").mkString("[", ", ", "]")
    }
    else {
      this.fullName
    }
  }
}


@JsonSerialize
@JsonDeserialize
final class ObjectTypeDescriptor[T](val typeName: String,
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


trait StreamLikeTypeDescriptor[T] extends TypeDescriptor[T]


@JsonSerialize
@JsonDeserialize
final class StreamTypeDescriptor(val recordType: TypeDescriptor[_]) extends StreamLikeTypeDescriptor[Any] {
  override val typeName: String = "Stream"

  override val fields: List[FieldDescriptor[_]] = List()

  override val genericArguments: List[TypeDescriptor[_]] = List(this.recordType)
}

object StreamTypeDescriptor {
  def unapply(arg: StreamTypeDescriptor): Option[TypeDescriptor[_]] = Some(arg.recordType)
}


@JsonSerialize
@JsonDeserialize
final class JoinedStreamsTypeDescriptor(val leftRecordType: TypeDescriptor[_],
                                        val rightRecordType: TypeDescriptor[_]) extends StreamLikeTypeDescriptor[Any] {
  override val typeName: String = "JoinedStreams"

  override val fields: List[FieldDescriptor[_]] = List(
    FieldDescriptor("left", this.leftRecordType),
    FieldDescriptor("right", this.rightRecordType))

  override val genericArguments: List[TypeDescriptor[_]] = List(this.leftRecordType, this.rightRecordType)
}

object JoinedStreamsTypeDescriptor {
  def unapply(arg: JoinedStreamsTypeDescriptor): Option[(TypeDescriptor[_], TypeDescriptor[_])] = Some((arg.leftRecordType, arg.rightRecordType))
}


final class GroupedStreamTypeDescriptor(val recordType: TypeDescriptor[_]) extends StreamLikeTypeDescriptor[Any] {
  override val typeName: String = "GroupedStream"

  override val fields: List[FieldDescriptor[_]] = List()

  override val genericArguments: List[TypeDescriptor[_]] = List(this.recordType)
}

object GroupedStreamTypeDescriptor {
  def unapply(arg: GroupedStreamTypeDescriptor): Option[TypeDescriptor[_]] = Some(arg.recordType)
}
