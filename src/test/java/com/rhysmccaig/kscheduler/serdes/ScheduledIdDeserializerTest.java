package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

public class ScheduledIdDeserializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH.plus(Duration.ofNanos(123456)).plus(Duration.ofDays(1000));
  private static String ID = "1234";
  private static String DELIMITER = "~";

  private static ScheduledIdDeserializer DESERIALIZER = new ScheduledIdDeserializer();

  @Test
  public void deserialize() {
    var bytes = new StringBuilder()
      .append(SCHEDULED.getEpochSecond())
      .append(DELIMITER)
      .append(SCHEDULED.getNano())
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    var expected = new ScheduledId(SCHEDULED, ID);
    assertEquals(expected, DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_no_id() {
    var bytes = new StringBuilder()
      .append(SCHEDULED.getEpochSecond())
      .append(DELIMITER)
      .append(SCHEDULED.getNano())
      .append(DELIMITER)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    var expected = new ScheduledId(SCHEDULED, null);
    assertEquals(expected, DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_no_nanos_throws() {
    var bytes = new StringBuilder()
      .append(SCHEDULED.getEpochSecond())
      .append(DELIMITER)
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_no_seconds_throws() {
    var bytes = new StringBuilder()
      .append(DELIMITER)
      .append(SCHEDULED.getNano())
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_seconds_wrong_type_throws() {
    var bytes = new StringBuilder()
      .append("X")
      .append(DELIMITER)
      .append(SCHEDULED.getNano())
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_nanos_wrong_type_throws() {
    var bytes = new StringBuilder()
      .append(SCHEDULED.getEpochSecond())
      .append(DELIMITER)
      .append("X")
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_overflow_throws() {
    var bytes = new StringBuilder()
      .append(Long.MAX_VALUE)
      .append(DELIMITER)
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_underflow_throws() {
    var bytes = new StringBuilder()
      .append(Long.MIN_VALUE)
      .append(DELIMITER)
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_extra_field_throws() {
    var bytes = new StringBuilder()
      .append(SCHEDULED.getEpochSecond())
      .append(DELIMITER)
      .append(SCHEDULED.getNano())
      .append(DELIMITER)
      .append(ID)
      .append(DELIMITER)
      .append("X")
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

}