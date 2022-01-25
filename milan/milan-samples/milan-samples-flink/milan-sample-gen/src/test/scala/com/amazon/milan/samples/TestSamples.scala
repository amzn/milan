package com.amazon.milan.samples

import com.amazon.milan.tools.InstanceParameters
import org.junit.Test


@Test
class TestSamples {
  @Test
  def test_GroupByFlatMapSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new GroupByFlatMapSample().getApplicationInstance(InstanceParameters.empty)
    getTestApplicationExecutor.executeApplication(instance, 60)
  }

  @Test
  def test_GroupBySelectSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new GroupBySelectSample().getApplicationInstance(InstanceParameters.empty)
    getTestApplicationExecutor.executeApplication(instance, 60)
  }

  @Test
  def test_FullJoinSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new FullJoinSample().getApplicationInstance(InstanceParameters.empty)
    getTestApplicationExecutor.executeApplication(instance, 120)
  }

  @Test
  def test_TimeWindowSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new TimeWindowSample().getApplicationInstance(InstanceParameters.empty)
    getTestApplicationExecutor.executeApplication(instance, 60)
  }
}
