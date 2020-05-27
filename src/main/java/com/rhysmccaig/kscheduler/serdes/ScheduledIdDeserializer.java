package com.rhysmccaig.kscheduler.serdes;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledIdDeserializer implements Deserializer<ScheduledId> {
    
  private static final String DELIMITER = "~";

  public ScheduledId deserialize(String topic, byte[] bytes) {
    ScheduledId scheduledId = null;
    if (bytes != null) {
      var str = new String(bytes, StandardCharsets.UTF_8);
      var tokens = str.split(DELIMITER);
      if (tokens.length == 3) {
        try {
          var seconds = Long.parseLong(tokens[0]);
          var nanos = Integer.parseInt(tokens[1]);
          var id = tokens[3];
          var scheduled = Instant.ofEpochSecond(seconds, nanos);
          scheduledId = new ScheduledId(scheduled, id);
        } catch (NumberFormatException | ArithmeticException | DateTimeException ex) {
          throw new SerializationException(ex);
        }
      } 
    }
    return scheduledId;
  }

}