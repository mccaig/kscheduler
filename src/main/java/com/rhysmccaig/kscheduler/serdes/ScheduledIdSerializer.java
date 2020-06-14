package com.rhysmccaig.kscheduler.serdes;

import java.nio.ByteBuffer;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.util.SerializationUtils;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledIdSerializer implements Serializer<ScheduledId> {

  private static int INSTANT_SIZE = Long.BYTES + Integer.BYTES;
  private static int ID_SIZE = Long.BYTES + Long.BYTES;
  private static final ThreadLocal<ByteBuffer> _THREADLOCAL = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INSTANT_SIZE + ID_SIZE));

  public byte[] serialize(String topic, ScheduledId data) {
    if (data == null) {
      return null;
    } else {
      var id = data.id();
      var buffer = _THREADLOCAL.get().position(0);
      buffer.put(SerializationUtils.toOrderedBytes(data.scheduled().getEpochSecond()))
            .put(SerializationUtils.toOrderedBytes(data.scheduled().getNano()));
      if (data.id() != null) {
        buffer.putLong(id.getMostSignificantBits())
              .putLong(id.getLeastSignificantBits());
      }
      buffer.flip();
      var bytes = new byte[buffer.limit()];
      buffer.get(bytes, 0, bytes.length);
      return bytes;
    }
  }

}