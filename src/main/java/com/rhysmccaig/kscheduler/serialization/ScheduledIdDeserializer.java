package com.rhysmccaig.kscheduler.serialization;

import static com.rhysmccaig.kscheduler.util.SerializationUtils.getUUID;
import static com.rhysmccaig.kscheduler.util.SerializationUtils.getInstant;

import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledIdDeserializer implements Deserializer<ScheduledId> {
    
  private static byte VERSION_BYTE = ScheduledIdSerializer.VERSION_BYTE;
  private static int SERIALIZED_SIZE = ScheduledIdSerializer.SERIALIZED_SIZE;
  private static final ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(SERIALIZED_SIZE));
  
  public ScheduledId deserialize(byte[] bytes) {
    return deserialize(null, bytes);
  }

  public ScheduledId deserialize(String topic, byte[] bytes) {
    if (bytes == null){
      return null;
    } else if (bytes.length != SERIALIZED_SIZE){
      throw new SerializationException("Invalid byte count!");
    }
    var buffer = TL_BUFFER.get().position(0);
    buffer.put(bytes).flip();
    var version = buffer.get();
    if (version != VERSION_BYTE) {
      throw new SerializationException("Unsupported version");
    }
    Instant scheduled;
    try {
      scheduled = getInstant(buffer);
    } catch (DateTimeException ex) {
      throw new SerializationException(ex);
    }
    var id = getUUID(buffer);
    return new ScheduledId(scheduled, id);
  }

}