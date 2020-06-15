package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.util.SerializationUtils;

import org.junit.jupiter.api.Test;

public class ScheduledIdSerializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH.plus(Duration.ofNanos(123456)).plus(Duration.ofDays(1000));
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");

  private static ScheduledIdSerializer SERIALIZER = new ScheduledIdSerializer();

  @Test
  public void serialize() {
    var buffer = ByteBuffer.allocate(29)
        .put(ScheduledIdSerializer.VERSION_BYTE)
        .put(SerializationUtils.toOrderedBytes(SCHEDULED.getEpochSecond()))
        .put(SerializationUtils.toOrderedBytes(SCHEDULED.getNano()))
        .putLong(ID.getMostSignificantBits())
        .putLong(ID.getLeastSignificantBits());
    var expected = buffer.array();
    var record = new ScheduledId(SCHEDULED, ID);
    var actual = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void serialize_no_id() {
    var buffer = ByteBuffer.allocate(13)
        .put(ScheduledIdSerializer.VERSION_BYTE)
        .put(SerializationUtils.toOrderedBytes(SCHEDULED.getEpochSecond()))
        .put(SerializationUtils.toOrderedBytes(SCHEDULED.getNano()));
    var expected = buffer.array();
    var record = new ScheduledId(SCHEDULED, null);
    var actual = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, actual);
  }
}