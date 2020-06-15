package com.rhysmccaig.kscheduler.serdes;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Serde;

public class ScheduledIdSerde implements Serde<ScheduledId> {
    
  public static ScheduledIdDeserializer DESERIALIZER = new ScheduledIdDeserializer();
  public static ScheduledIdSerializer SERIALIZER = new ScheduledIdSerializer();

  public ScheduledIdDeserializer deserializer() {
    return DESERIALIZER;
  }

  public ScheduledIdSerializer serializer() {
    return SERIALIZER;
  }
}