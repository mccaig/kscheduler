package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.util.SerializationUtils;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

public class ScheduledIdDeserializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH.plus(Duration.ofNanos(123456)).plus(Duration.ofDays(1000));
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");

  private static ScheduledIdDeserializer DESERIALIZER = new ScheduledIdDeserializer();

  @Test
  public void deserialize() {
    var buffer = ByteBuffer.allocate(29)
      .put(ScheduledIdSerializer.VERSION_BYTE)
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getEpochSecond()))
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getNano()))
      .putLong(ID.getMostSignificantBits())
      .putLong(ID.getLeastSignificantBits());
    var bytes = buffer.array();
    var expected = new ScheduledId(SCHEDULED, ID);
    var actual = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, actual);
  }

  @Test
  public void deserialize_no_id_throws() {
    var buffer = ByteBuffer.allocate(13)
      .put(ScheduledIdSerializer.VERSION_BYTE)
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getEpochSecond()))
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getNano()));
    var bytes = buffer.array();
    var expected = new ScheduledId(SCHEDULED, null);
    var actual = DESERIALIZER.deserialize(bytes);
    assertEquals(expected, actual);  
  }

  @Test
  public void deserialize_too_short_throws() {
    var buffer = ByteBuffer.allocate(9)
      .put(ScheduledIdSerializer.VERSION_BYTE)
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getEpochSecond()));
    var bytes = buffer.array();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }



  @Test
  public void deserialize_overflow_throws() {
    var buffer = ByteBuffer.allocate(29)
      .put(ScheduledIdSerializer.VERSION_BYTE)
      .put(SerializationUtils.toOrderedBytes(Long.MAX_VALUE))
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getNano()))
      .putLong(ID.getMostSignificantBits())
      .putLong(ID.getLeastSignificantBits());
    var bytes = buffer.array();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_underflow_throws() {
    var buffer = ByteBuffer.allocate(29)
      .put(ScheduledIdSerializer.VERSION_BYTE)
      .put(SerializationUtils.toOrderedBytes(Long.MIN_VALUE))
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getNano()))
      .putLong(ID.getMostSignificantBits())
      .putLong(ID.getLeastSignificantBits());
    var bytes = buffer.array();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_invalid_version_throws() {
    byte version = 0x55;
    var buffer = ByteBuffer.allocate(29)
      .put(version)
      .put(SerializationUtils.toOrderedBytes(Long.MIN_VALUE))
      .put(SerializationUtils.toOrderedBytes(SCHEDULED.getNano()))
      .putLong(ID.getMostSignificantBits())
      .putLong(ID.getLeastSignificantBits());
    var bytes = buffer.array();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

}