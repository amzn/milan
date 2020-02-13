package com.amazon.milan.typeutil

import scala.reflect.macros.whitebox


trait LiftTypeDescriptorHost {
  val c: whitebox.Context

  import c.universe._

  implicit def liftNumericTypeDescriptor[T: c.WeakTypeTag]: Liftable[NumericTypeDescriptor[T]] = { t =>
    q"new ${weakTypeOf[NumericTypeDescriptor[T]]}(${t.typeName})"
  }

  implicit def liftBasicTypeDescriptor[T: c.WeakTypeTag]: Liftable[BasicTypeDescriptor[T]] = { tree =>
    tree match {
      case t: NumericTypeDescriptor[T] => liftNumericTypeDescriptor[T](c.weakTypeTag[T])(t)
      case t: BasicTypeDescriptor[T] => q"new ${weakTypeOf[BasicTypeDescriptor[T]]}(${t.typeName})"
    }
  }

  implicit def liftTupleTypeDescriptor[T: c.WeakTypeTag]: Liftable[TupleTypeDescriptor[T]] = { t =>
    q"new ${weakTypeOf[TupleTypeDescriptor[T]]}(${t.typeName}, ${t.genericArguments}, ${t.fields})"
  }

  implicit def liftObjectTypeDescriptor[T: c.WeakTypeTag]: Liftable[ObjectTypeDescriptor[T]] = { t =>
    q"new ${weakTypeOf[ObjectTypeDescriptor[T]]}(${t.typeName}, ${t.genericArguments}, ${t.fields})"
  }

  implicit def liftCollectionTypeDescriptor[T: c.WeakTypeTag]: Liftable[CollectionTypeDescriptor[T]] = { t =>
    q"new ${weakTypeOf[CollectionTypeDescriptor[T]]}(${t.typeName}, ${t.genericArguments})"
  }

  implicit val liftDataStreamTypeDescriptor: Liftable[DataStreamTypeDescriptor] = { t =>
    q"new ${typeOf[DataStreamTypeDescriptor]}(${t.recordType})"
  }

  implicit val liftJoinedStreamsTypeDescriptor: Liftable[JoinedStreamsTypeDescriptor] = { t =>
    q"new ${typeOf[JoinedStreamsTypeDescriptor]}(${t.leftRecordType}, ${t.rightRecordType})"
  }

  implicit val liftGroupedStreamTypeDescriptor: Liftable[GroupedStreamTypeDescriptor] = { t =>
    q"new ${typeOf[GroupedStreamTypeDescriptor]}(${t.recordType})"
  }

  implicit def liftStreamTypeDescriptor: Liftable[StreamTypeDescriptor] = { tree =>
    tree match {
      case t: DataStreamTypeDescriptor => liftDataStreamTypeDescriptor(t)
      case t: JoinedStreamsTypeDescriptor => liftJoinedStreamsTypeDescriptor(t)
      case t: GroupedStreamTypeDescriptor => liftGroupedStreamTypeDescriptor(t)
    }
  }

  implicit def liftTypeDescriptor[T: c.WeakTypeTag]: Liftable[TypeDescriptor[T]] = { tree =>
    tree match {
      case t: BasicTypeDescriptor[T] => liftBasicTypeDescriptor[T](c.weakTypeTag[T])(t)
      case t: TupleTypeDescriptor[T] => liftTupleTypeDescriptor(c.weakTypeTag[T])(t)
      case t: ObjectTypeDescriptor[T] => liftObjectTypeDescriptor(c.weakTypeTag[T])(t)
      case t: CollectionTypeDescriptor[T] => liftCollectionTypeDescriptor(c.weakTypeTag[T])(t)
      case t: StreamTypeDescriptor => liftStreamTypeDescriptor(t)
    }
  }

  implicit val liftTypeDescriptorAny: Liftable[TypeDescriptor[Any]] = { t =>
    liftTypeDescriptor[Any](c.weakTypeTag[Any])(t)
  }

  implicit val liftTypeDescriptorWildcard: Liftable[TypeDescriptor[_]] = { t =>
    liftTypeDescriptor[Any](c.weakTypeTag[Any])(t.asInstanceOf[TypeDescriptor[Any]])
  }

  implicit def liftFieldDescriptor[T: c.WeakTypeTag]: Liftable[FieldDescriptor[T]] = { t =>
    q"new ${weakTypeOf[FieldDescriptor[T]]}(${t.name}, ${t.fieldType})"
  }

  implicit val liftFieldDescriptorWildcard: Liftable[FieldDescriptor[_]] = { t =>
    q"new ${weakTypeOf[FieldDescriptor[Any]]}(${t.name}, ${t.fieldType.asInstanceOf[TypeDescriptor[Any]]})"
  }
}


