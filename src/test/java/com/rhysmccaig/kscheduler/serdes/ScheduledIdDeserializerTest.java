package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

public class ScheduledIdDeserializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH.plus(Duration.ofNanos(123456)).plus(Duration.ofDays(1000));
  private static String ID = "1234";

  private static ScheduledIdDeserializer DESERIALIZER = new ScheduledIdDeserializer();

  @Test
  public void deserialize() {
    var buffer = ByteBuffer.allocate(16)
      .putLong(SCHEDULED.getEpochSecond())
      .putInt(SCHEDULED.getNano())
      .put(ID.getBytes(StandardCharsets.UTF_8), 0, 4);
    var bytes = buffer.array();
    var expected = new ScheduledId(SCHEDULED, ID);
    var actual = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, actual);
  }

  @Test
  public void deserialize_no_id() {
    var buffer = ByteBuffer.allocate(12)
      .putLong(SCHEDULED.getEpochSecond())
      .putInt(SCHEDULED.getNano());
    var bytes = buffer.array();
    var expected = new ScheduledId(SCHEDULED, null);
    var actual = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, actual);
  }

  @Test
  public void deserialize_too_short_throws() {
    var buffer = ByteBuffer.allocate(11)
      .putLong(SCHEDULED.getEpochSecond());
    var bytes = buffer.array();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }



  @Test
  public void deserialize_overflow_throws() {
    var buffer = ByteBuffer.allocate(16)
      .putLong(Long.MAX_VALUE)
      .putInt(SCHEDULED.getNano())
      .put(ID.getBytes(StandardCharsets.UTF_8), 0, 4);
    var bytes = buffer.array();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_underflow_throws() {
    var buffer = ByteBuffer.allocate(16)
      .putLong(Long.MIN_VALUE)
      .putInt(SCHEDULED.getNano())
      .put(ID.getBytes(StandardCharsets.UTF_8), 0, 4);
    var bytes = buffer.array();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

}