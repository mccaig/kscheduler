package com.rhysmccaig.kscheduler.serdes;

import java.lang.StringBuilder;
import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledIdSerializer implements Serializer<ScheduledId> {

  private static final String DELIMITER = "~";

  public byte[] serialize(String topic, ScheduledId data) {
    byte[] bytes = null;
    if (data != null) {
      var seconds = data.scheduled().getEpochSecond();
      var nanos = data.scheduled().getNano();
      var sb = new StringBuilder()
        .append(seconds)
        .append(DELIMITER)
        .append(nanos)
        .append(DELIMITER);
      if (data.id() != null) {
        sb.append(data.id());
      }
      bytes = sb.toString().getBytes();
    }
    return bytes;
  }

}