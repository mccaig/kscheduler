package com.rhysmccaig.kscheduler.serdes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledIdDeserializer  implements Deserializer<ScheduledId> {
    
  private static final String DELIMITER = "~";

  public ScheduledId deserialize(String topic, byte[] data) {
    var combined = new String(data, StandardCharsets.UTF_8);
    var tokens = combined.split(DELIMITER);
    var seconds = Long.parseLong(tokens[0]);
    var nanos = Integer.parseInt(tokens[1]);
    var id = tokens[3];
    var scheduled = Instant.ofEpochSecond(seconds, nanos);
    return new ScheduledId(scheduled, id);
  }
}