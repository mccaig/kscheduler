package com.rhysmccaig.kscheduler.serialization;

import static com.rhysmccaig.kscheduler.util.SerializationUtils.putOrderedBytes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataDeserializerTest {

  private static Instant SCHEDULED = Instant.EPOCH;
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";
  private static ScheduledRecordMetadataDeserializer DESERIALIZER = new ScheduledRecordMetadataDeserializer();
 
  private static ByteBuffer buffer = ByteBuffer.allocate(ScheduledRecordMetadataSerializer.MAXIMUM_SERIALIZED_SIZE);

  private byte[] scheduledBytes = new byte[12];
  private byte[] expiresBytes = new byte[12];
  private byte[] createdBytes = new byte[12];
  private byte[] idBytes = new byte[16];
  private byte[] destinationBytes;

  /**
   * Test setup.
   */
  @BeforeEach
  public void beforeEach() {
    putOrderedBytes(buffer.clear(), SCHEDULED).flip().get(scheduledBytes);
    putOrderedBytes(buffer.clear(), EXPIRES).flip().get(expiresBytes);
    putOrderedBytes(buffer.clear(), CREATED).flip().get(createdBytes);
    putOrderedBytes(buffer.clear(), ID).flip().get(idBytes);
    destinationBytes = DESTINATION.getBytes(UTF_8);
  }

  @Test
  public void desserialize() {
    buffer.clear()
          .put(ScheduledRecordMetadataSerializer.VERSION_BYTE)
          .put(scheduledBytes)
          .put(expiresBytes)
          .put(createdBytes)
          .put(idBytes)
          .put(destinationBytes)
          .flip();
    var bytes = new byte[buffer.limit()];
    buffer.get(bytes);
    var expected = new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);
    var actual = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, actual);
  }

  @Test
  public void desserialize_null_returns_null() {
    var fromBytes = DESERIALIZER.deserialize(null, null);
    assertEquals(null, fromBytes);
  }

  @Test
  public void desserialize_too_short_throws() {
    var size = ScheduledRecordMetadataSerializer.MINIMUM_SERIALIZED_SIZE - 1;
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, new byte[size]));
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, new byte[0]));
  }

  @Test
  public void desserialize_too_long_throws() {
    var size = ScheduledRecordMetadataSerializer.MAXIMUM_SERIALIZED_SIZE + 1;
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, new byte[size]));
  }

  @Test
  public void desserialize_bad_version_throws() {
    buffer.clear()
          .put((byte) 0x55)
          .put(scheduledBytes)
          .put(expiresBytes)
          .put(createdBytes)
          .put(idBytes)
          .put(destinationBytes)
          .flip();
    var bytes = new byte[buffer.limit()];
    buffer.get(bytes);
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }



}