package com.rhysmccaig.kscheduler.serialization;

import static com.rhysmccaig.kscheduler.util.SerializationUtils.putOrderedBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class ScheduledRecordMetadataSerializer implements Serializer<ScheduledRecordMetadata> {
    
  public static final byte VERSION_BYTE = 0x00;
  private static final int INSTANT_SIZE = Long.BYTES + Integer.BYTES;
  private static final int ID_SIZE = Long.BYTES + Long.BYTES;
  private static final int MAX_DESTINATION_SIZE = 249;
  public static final int MINIMUM_SERIALIZED_SIZE = 1 + (3 * INSTANT_SIZE) + ID_SIZE;
  public static final int MAXIMUM_SERIALIZED_SIZE = MINIMUM_SERIALIZED_SIZE + MAX_DESTINATION_SIZE;
  private static final ThreadLocal<ByteBuffer> TL_BUFFER = 
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(MAXIMUM_SERIALIZED_SIZE));

  public byte[] serialize(ScheduledRecordMetadata data) {
    return serialize(null, data);
  }

  /**
   * Deserializes ScheduledRecordMetadata into bytes.
   */
  public byte[] serialize(String topic, ScheduledRecordMetadata data) {
    if (data == null) {
      return null;
    } else {
      var buffer = TL_BUFFER.get().clear();
      buffer.put(VERSION_BYTE);
      putOrderedBytes(buffer, data.scheduled());
      putOrderedBytes(buffer, data.expires());
      putOrderedBytes(buffer, data.created());
      putOrderedBytes(buffer, data.id());
      try {
        var destinationBytes = data.destination().getBytes(UTF_8);
        buffer.put(destinationBytes);
      } catch (BufferOverflowException ex) {
        // Kafka topics names are limited to 249 caracters with chars A-Z, a-z, 0-9, _, -, .
        // These all fit into a single byte when encoded with UTF-8, so a buffer overflow 
        // would only occur if there was an invalid topic name.
        throw new SerializationException("Destination was too long to be encoded, this is an invalid topic name");
      }
      buffer.flip();
      var bytes = new byte[buffer.limit()];
      buffer.get(bytes);
      return bytes;
    }
  }

}