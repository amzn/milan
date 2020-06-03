package com.amazon.milan.flink.generator

import com.amazon.milan.flink.runtime.RuntimeUtil
import com.amazon.milan.flink.testing.IntRecord
import org.junit.Assert._
import org.junit.Test


@Test
class TestDataSourceGenerator {
  @Test
  def test_DataSourceGenerator_LoadJson(): Unit = {
    val l = RuntimeUtil.loadJsonList[IntRecord]("[{\"i\":1,\"recordId\":\"fd121d31-3c86-449c-a606-f848d16cafdf\"}]")
    assertEquals(List(IntRecord(1)), l)
  }
}
