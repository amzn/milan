package com.amazon.milan.aws.serverless.runtime

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.util
import scala.collection.JavaConverters._

/**
 * Helper methods for creating DynamoDb AttributeValue objects.
 *
 * Why do these return Any instead of AttributeValue?
 * Because otherwise the tests that do runtime code evaluation fail with the
 * error 'illegal cyclic reference involving class AttributeValue'.
 */
object DynamoDb {
  def s(value: String): Any =
    AttributeValue.builder().s(value).build()

  def n(value: String): Any =
    AttributeValue.builder().n(value).build()

  def b(value: Boolean): Any =
    AttributeValue.builder().bool(value).build()

  def l(items: Iterable[Any]): Any =
    AttributeValue.builder().l(items.map(_.asInstanceOf[AttributeValue]).asJavaCollection).build()

  def m(fields: util.HashMap[String, Any]): Any = {
    AttributeValue.builder().m(fields.asInstanceOf[util.HashMap[String, AttributeValue]]).build()
  }
}
