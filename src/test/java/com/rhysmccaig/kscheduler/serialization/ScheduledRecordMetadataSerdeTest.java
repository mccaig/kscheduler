package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import static com.rhysmccaig.kscheduler.util.SerializationUtils.putOrderedBytes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataSerdeTest {
  
  private static Serde<ScheduledRecordMetadata> SERDE = new ScheduledRecordMetadataSerde();
  private static Instant SCHEDULED = Instant.EPOCH;
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";
  private static Deserializer<ScheduledRecordMetadata> DESERIALIZER = SERDE.deserializer();
  private static Serializer<ScheduledRecordMetadata> SERIALIZER = SERDE.serializer();

  @Test
  public void roundtrip() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);
    var actual = DESERIALIZER.deserialize(null, SERIALIZER.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_reverse() {
    var buffer = ByteBuffer.allocate(ScheduledRecordMetadataSerializer.MAXIMUM_SERIALIZED_SIZE);
    buffer.put(ScheduledRecordMetadataSerializer.VERSION_BYTE);
    putOrderedBytes(buffer, SCHEDULED);
    putOrderedBytes(buffer, EXPIRES);
    putOrderedBytes(buffer, CREATED);
    putOrderedBytes(buffer, ID);
    buffer.put(DESTINATION.getBytes(StandardCharsets.UTF_8));
    buffer.flip();
    var expected = new byte[buffer.limit()];
    buffer.get(expected);
    var actual = SERIALIZER.serialize(null, DESERIALIZER.deserialize(null, expected));
    assertArrayEquals(expected, actual);
  }

}