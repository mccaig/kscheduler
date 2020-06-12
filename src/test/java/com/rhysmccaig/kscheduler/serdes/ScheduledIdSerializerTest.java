package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.junit.jupiter.api.Test;

public class ScheduledIdSerializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH.plus(Duration.ofNanos(123456)).plus(Duration.ofDays(1000));
  private static String ID = "1234";

  private static ScheduledIdSerializer SERIALIZER = new ScheduledIdSerializer();

  @Test
  public void serialize() {
    var buffer = ByteBuffer.allocate(16)
        .putLong(SCHEDULED.getEpochSecond())
        .putInt(SCHEDULED.getNano())
        .put(ID.getBytes(StandardCharsets.UTF_8), 0, 4);
    var expected = buffer.array();
    var record = new ScheduledId(SCHEDULED, ID);
    var actual = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void serialize_no_id() {
    var buffer = ByteBuffer.allocate(16)
        .putLong(SCHEDULED.getEpochSecond())
        .putInt(SCHEDULED.getNano());
    var expected = buffer.array();
    var record = new ScheduledId(SCHEDULED, null);
    var actual = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, actual);
  }
}