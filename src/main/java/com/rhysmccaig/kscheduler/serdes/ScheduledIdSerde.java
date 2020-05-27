package com.rhysmccaig.kscheduler.serdes;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.apache.kafka.common.serialization.Serde;

public class ScheduledIdSerde implements Serde<ScheduledId> {
    
  public ScheduledIdDeserializer deserializer() {
    return new ScheduledIdDeserializer();
  }

  public ScheduledIdSerializer serializer() {
    return new ScheduledIdSerializer();
  }
}