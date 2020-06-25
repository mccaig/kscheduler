package com.rhysmccaig.kscheduler.serialization;

import static com.rhysmccaig.kscheduler.util.SerializationUtils.getInstant;
import static com.rhysmccaig.kscheduler.util.SerializationUtils.getUuid;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledRecordMetadataDeserializer implements Deserializer<ScheduledRecordMetadata> {
  
  private static int VERSION_BYTE = ScheduledRecordMetadataSerializer.VERSION_BYTE;
  private static int MINIMUM_SERIALIZED_SIZE = ScheduledRecordMetadataSerializer.MINIMUM_SERIALIZED_SIZE;
  private static int MAXIMUM_SERIALIZED_SIZE = ScheduledRecordMetadataSerializer.MAXIMUM_SERIALIZED_SIZE;
  private static final ThreadLocal<ByteBuffer> TL_BUFFER = 
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(MAXIMUM_SERIALIZED_SIZE));

  public ScheduledRecordMetadata deserialize(byte[] bytes) {
    return deserialize(null, bytes);
  }
  
  /**
   * Deserializes bytes into ScheduledRecordMetadata.
   */
  public ScheduledRecordMetadata deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    } else if (bytes.length < MINIMUM_SERIALIZED_SIZE) {
      throw new SerializationException("Not enough bytes to be ScheduledRecordMetadata");
    } else if (bytes.length > MAXIMUM_SERIALIZED_SIZE) {
      throw new SerializationException("Too many bytes to be ScheduledRecordMetadata");
    }
    var buffer = TL_BUFFER.get().clear();
    buffer.put(bytes).flip();
    var version = buffer.get();
    if (version != VERSION_BYTE) {
      throw new SerializationException("Unsupported version");
    }
    Instant scheduled;
    Instant expires;
    Instant created;
    try {
      scheduled = getInstant(buffer);
      expires = getInstant(buffer);
      created = getInstant(buffer);
    } catch (DateTimeException ex) {
      throw new SerializationException(ex);
    }
    var id = getUuid(buffer);
    var destinationBytes = new byte[bytes.length - MINIMUM_SERIALIZED_SIZE];
    buffer.get(destinationBytes);
    var destination = new String(destinationBytes, UTF_8);
    return new ScheduledRecordMetadata(scheduled, expires, created, id, destination);
  }

}