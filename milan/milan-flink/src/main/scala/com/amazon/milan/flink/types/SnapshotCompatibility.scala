package com.amazon.milan.flink.types

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility}

object SnapshotCompatibility {
  def resolveCompatibility[T](createSerializer: List[TypeSerializer[_]] => TypeSerializer[T],
                              serializersAndResults: (TypeSerializer[_], TypeSerializerSchemaCompatibility[_])*): TypeSerializerSchemaCompatibility[T] = {
    val results = serializersAndResults.map(_._2)
    if (results.exists(_.isIncompatible)) {
      TypeSerializerSchemaCompatibility.incompatible()
    }
    else if (results.forall(c => c.isCompatibleAfterMigration || c.isCompatibleAsIs)) {
      TypeSerializerSchemaCompatibility.compatibleAfterMigration()
    }
    else if (results.forall(c => c.isCompatibleWithReconfiguredSerializer || c.isCompatibleAsIs)) {
      val newSerializers =
        serializersAndResults.map {
          case (serializer, result) =>
            if (result.isCompatibleWithReconfiguredSerializer) {
              result.getReconfiguredSerializer
            }
            else {
              serializer
            }
        }
      val reconfiguredSerializer = createSerializer(newSerializers.toList)
      TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(reconfiguredSerializer)
    }
    else if (results.forall(_.isCompatibleAsIs)) {
      TypeSerializerSchemaCompatibility.compatibleAsIs()
    }
    else {
      TypeSerializerSchemaCompatibility.incompatible()
    }
  }

}
