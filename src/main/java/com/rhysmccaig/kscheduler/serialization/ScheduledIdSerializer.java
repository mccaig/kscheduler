package com.rhysmccaig.kscheduler.serialization;

import static com.rhysmccaig.kscheduler.util.SerializationUtils.putOrderedBytes;

import java.nio.ByteBuffer;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledIdSerializer implements Serializer<ScheduledId> {

  public static byte VERSION_BYTE = 0x00;
  private static int INSTANT_SIZE = Long.BYTES + Integer.BYTES;
  private static int ID_SIZE = Long.BYTES + Long.BYTES;
  public static int SERIALIZED_SIZE = (1 + INSTANT_SIZE + ID_SIZE);
  private static final ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(SERIALIZED_SIZE));

  public byte[] serialize(ScheduledId data) {
    return serialize(null, data);
  }

  public byte[] serialize(String topic, ScheduledId data) {
    if (data == null) {
      return null;
    }
    var buffer = TL_BUFFER.get().position(0);
    buffer.put(VERSION_BYTE);
    putOrderedBytes(buffer, data.scheduled());
    putOrderedBytes(buffer, data.id());
    buffer.flip();
    var bytes = new byte[buffer.limit()];
    buffer.get(bytes, 0, bytes.length);
    return bytes;
  }

}