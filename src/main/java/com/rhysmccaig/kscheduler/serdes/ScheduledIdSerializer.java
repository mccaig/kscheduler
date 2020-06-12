package com.rhysmccaig.kscheduler.serdes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledIdSerializer implements Serializer<ScheduledId> {

  private static int INSTANT_SIZE = Long.BYTES + Integer.BYTES;

  public byte[] serialize(String topic, ScheduledId data) {
    if (data != null) {
      var idBytes = Objects.isNull(data.id()) ? new byte[] {} : data.id().getBytes(StandardCharsets.UTF_8);
      var buffer = ByteBuffer.allocate(INSTANT_SIZE + idBytes.length)
          .putLong(data.scheduled().getEpochSecond())
          .putInt(data.scheduled().getNano())
          .put(idBytes);
      return buffer.array();
    } else {
      return null;
    }
  }

}