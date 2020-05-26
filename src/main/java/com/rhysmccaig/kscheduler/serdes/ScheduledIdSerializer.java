package com.rhysmccaig.kscheduler.serdes;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Serializer;


public class ScheduledIdSerializer implements Serializer<ScheduledId> {

  // Last printable character in US-ASCII so fits in one byte in UTF-8
  // As other values are expected to be US-ASCII this allows for lexicographical ordering of the encoded bytes
  private static final String DELIMITER = "~";
  public byte[] serialize(String topic, ScheduledId sid) {
    var scheduled = sid.scheduled();
    var id = sid.id();
    var seconds = scheduled.getEpochSecond();
    var nanos = scheduled.getNano();
    var combined = new StringBuilder()
        .append(seconds)
        .append(DELIMITER)
        .append(nanos)
        .append(DELIMITER)
        .append(id)
        .toString();
    return combined.getBytes(StandardCharsets.UTF_8);
  }
    
}