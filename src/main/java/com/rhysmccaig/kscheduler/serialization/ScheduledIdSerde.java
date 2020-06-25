package com.rhysmccaig.kscheduler.serialization;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import org.apache.kafka.common.serialization.Serde;

public class ScheduledIdSerde implements Serde<ScheduledId> {
    
  private static ScheduledIdDeserializer DESERIALIZER = new ScheduledIdDeserializer();
  private static ScheduledIdSerializer SERIALIZER = new ScheduledIdSerializer();

  public ScheduledIdDeserializer deserializer() {
    return DESERIALIZER;
  }

  public ScheduledIdSerializer serializer() {
    return SERIALIZER;
  }
}