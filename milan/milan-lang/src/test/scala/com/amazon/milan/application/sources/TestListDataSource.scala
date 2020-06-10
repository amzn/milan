package com.amazon.milan.application.sources

import com.amazon.milan.application.DataSource
import com.amazon.milan.serialization.MilanObjectMapper
import com.amazon.milan.typeutil.createTypeDescriptor
import org.junit.Assert._
import org.junit.Test


object TestListDataSource {

  case class Record(i: Int)

}

import com.amazon.milan.application.sources.TestListDataSource._


@Test
class TestListDataSource {
  @Test
  def test_ListDataSource_AfterSerializationAndDeserializaton_ContainsEquivalentElements(): Unit = {
    val original = new ListDataSource(List(Record(1), Record(2)))
    val originalAsSource = original.asInstanceOf[DataSource[Record]]
    val copy = MilanObjectMapper.copy(originalAsSource)
    assertEquals(original.values, copy.asInstanceOf[ListDataSource[Record]].values)
  }
}
