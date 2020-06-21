package com.rhysmccaig.kscheduler.serialization;

import static com.rhysmccaig.kscheduler.util.SerializationUtils.putOrderedBytes;

import java.nio.ByteBuffer;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledIdSerializer implements Serializer<ScheduledId> {

  public static final byte VERSION_BYTE = 0x00;
  private static final int INSTANT_SIZE = Long.BYTES + Integer.BYTES;
  private static final int ID_SIZE = Long.BYTES + Long.BYTES;
  public static final int MIN_SERIALIZED_SIZE = 1 + INSTANT_SIZE;
  public static final int MAX_SERIALIZED_SIZE = MIN_SERIALIZED_SIZE + ID_SIZE;

  private static final ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(MAX_SERIALIZED_SIZE));

  public byte[] serialize(ScheduledId data) {
    return serialize(null, data);
  }

  public byte[] serialize(String topic, ScheduledId data) {
    if (data == null) {
      return null;
    }
    var buffer = TL_BUFFER.get().clear();
    buffer.put(VERSION_BYTE);
    putOrderedBytes(buffer, data.scheduled());
    if (data.id() != null) {
      putOrderedBytes(buffer, data.id());
    }
    buffer.flip();
    var bytes = new byte[buffer.limit()];
    buffer.get(bytes);
    return bytes;
  }

}