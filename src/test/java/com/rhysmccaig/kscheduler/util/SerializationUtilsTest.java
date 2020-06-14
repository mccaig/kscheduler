package com.rhysmccaig.kscheduler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

public class SerializationUtilsTest {

  @Test
  public void orderedLongs() {
    List<Long> values = List.of(
      Long.MIN_VALUE, Long.MIN_VALUE + 1, -1000000L, -1000L, -100L, -2L, -1L,
      0L,
      1L, 2L, 100L, 1000L, 1000000L, Long.MAX_VALUE -1, Long.MAX_VALUE);
    values.forEach(val -> {
      var bytes = SerializationUtils.toOrderedBytes(val);
      var lng = SerializationUtils.longFromOrderedBytes(bytes);
      assertEquals(val, lng);
    });
  }

  @Test
  public void orderedIntegers() {
    List<Integer> values = List.of(
      Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1000000, -1000, -100, -2, -1,
      0,
      1, 2, 100, 1000, 1000000, Integer.MAX_VALUE -1, Integer.MAX_VALUE);
    values.forEach(val -> {
      var bytes = SerializationUtils.toOrderedBytes(val);
      var i = SerializationUtils.intFromOrderedBytes(bytes);
      assertEquals(val, i);
    });
  }

}