package com.amazon.milan.samples

import org.junit.Test


@Test
class TestSamples {
  @Test
  def test_GroupByFlatMapSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new GroupByFlatMapSample().getApplicationInstance(List.empty)
    getTestApplicationExecutor.executeApplication(instance, 60)
  }

  @Test
  def test_GroupBySelectSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new GroupBySelectSample().getApplicationInstance(List.empty)
    getTestApplicationExecutor.executeApplication(instance, 60)
  }

  @Test
  def test_FullJoinSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new FullJoinSample().getApplicationInstance(List.empty)
    getTestApplicationExecutor.executeApplication(instance, 120)
  }

  @Test
  def test_TimeWindowSample_GetApplication_ReturnsInstanceThatExecutes(): Unit = {
    val instance = new TimeWindowSample().getApplicationInstance(List.empty)
    getTestApplicationExecutor.executeApplication(instance, 60)
  }
}
