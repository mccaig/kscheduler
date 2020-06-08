package com.rhysmccaig.kscheduler.serdes;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Objects;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledIdDeserializer implements Deserializer<ScheduledId> {
    
  private static final String DELIMITER = "~";

  public ScheduledId deserialize(String topic, byte[] bytes) {
    ScheduledId scheduledId = null;
    if (Objects.nonNull(bytes)) {
      var str = new String(bytes, StandardCharsets.UTF_8);
      var tokens = str.split(DELIMITER);
      if (tokens.length < 2)
        throw new SerializationException("less than 2 tokens in bytes string");
      if (tokens.length > 3)
        throw new SerializationException("more than 3 tokens in bytes string");
      try {
        var seconds = Long.parseLong(tokens[0]);
        var nanos = Integer.parseInt(tokens[1]);
        String id = null;
        if (tokens.length == 3) {
          id = tokens[2];
        }
        var scheduled = Instant.ofEpochSecond(seconds, nanos);
        scheduledId = new ScheduledId(scheduled, id);
      } catch (NumberFormatException | ArithmeticException | DateTimeException ex) {
        throw new SerializationException(ex);
      }
    }
    return scheduledId;
  }

}