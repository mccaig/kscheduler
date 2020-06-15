package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

public class ScheduledIdSerdeTest {
  
  private static Serde<ScheduledId> SERDE = new ScheduledIdSerde();
  private static Instant SCHEDULED = Instant.EPOCH;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static Deserializer<ScheduledId> deserializer = SERDE.deserializer();
  private static Serializer<ScheduledId> serializer = SERDE.serializer();

  @Test
  public void roundtrip() {
    var expected = new ScheduledId(SCHEDULED, ID);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_id() {
    var expected = new ScheduledId(SCHEDULED, null);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

}