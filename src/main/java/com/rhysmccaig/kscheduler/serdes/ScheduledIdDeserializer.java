package com.rhysmccaig.kscheduler.serdes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Objects;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledIdDeserializer implements Deserializer<ScheduledId> {
    
  private static int INSTANT_SIZE = Long.BYTES + Integer.BYTES;

  public ScheduledId deserialize(String topic, byte[] bytes) {
    if (Objects.isNull(bytes)){
      return null;
    } else if (bytes.length < INSTANT_SIZE){
      throw new SerializationException("Not enough bytes to deserialize!");
    }
    var buffer = ByteBuffer.wrap(bytes);
    var seconds = buffer.getLong();
    var nanos = buffer.getInt();
    Instant scheduled;
    try {
      scheduled = Instant.ofEpochSecond(seconds, nanos);
    } catch (DateTimeException ex) {
      throw new SerializationException(ex);
    }
    var idLength = bytes.length - INSTANT_SIZE;
    final ScheduledId scheduledId;
    if (idLength > 0) {
      var id = new byte[idLength];
      buffer.get(id);
      var idString = new String(id, StandardCharsets.UTF_8);
      scheduledId = new ScheduledId(scheduled, idString);
    } else {
      scheduledId = new ScheduledId(scheduled, null);
    }
    return scheduledId;
  }

}