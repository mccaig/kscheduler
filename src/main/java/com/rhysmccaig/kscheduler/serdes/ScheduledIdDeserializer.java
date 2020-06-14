package com.rhysmccaig.kscheduler.serdes;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.util.SerializationUtils;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledIdDeserializer implements Deserializer<ScheduledId> {
    
  private static int INSTANT_SIZE = Long.BYTES + Integer.BYTES;
  private static int ID_SIZE = Long.BYTES + Long.BYTES;
  private static final ThreadLocal<ByteBuffer> _THREADLOCAL = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INSTANT_SIZE + ID_SIZE));

  public ScheduledId deserialize(String topic, byte[] bytes) {
    if (bytes == null){
      return null;
    } else if (bytes.length != INSTANT_SIZE && bytes.length != (INSTANT_SIZE + ID_SIZE)){
      throw new SerializationException("Invalid byte count!");
    }
    var buffer = _THREADLOCAL.get().position(0);
    buffer.put(bytes).flip();
    var secondsBytes = new byte[Long.BYTES];
    buffer.get(secondsBytes);
    var seconds = SerializationUtils.longFromOrderedBytes(secondsBytes);
    var nanosBytes = new byte[Integer.BYTES];
    buffer.get(nanosBytes);
    var nanos = SerializationUtils.intFromOrderedBytes(nanosBytes);
    Instant scheduled;
    try {
      scheduled = Instant.ofEpochSecond(seconds, nanos);
    } catch (DateTimeException ex) {
      throw new SerializationException(ex);
    }
    final ScheduledId scheduledId;
    if (bytes.length  == (INSTANT_SIZE + ID_SIZE)) {
      var mostSigBits = buffer.getLong();
      var leastSigBits = buffer.getLong();
      scheduledId = new ScheduledId(scheduled, new UUID(mostSigBits, leastSigBits));
    } else {
      scheduledId = new ScheduledId(scheduled, null);
    }
    return scheduledId;
  }

}