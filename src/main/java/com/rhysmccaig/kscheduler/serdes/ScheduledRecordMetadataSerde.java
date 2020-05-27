package com.rhysmccaig.kscheduler.serdes;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

import org.apache.kafka.common.serialization.Serde;

public class ScheduledRecordMetadataSerde implements Serde<ScheduledRecordMetadata> {
    
  public ScheduledRecordMetadataDeserializer deserializer() {
    return new ScheduledRecordMetadataDeserializer();
  }

  public ScheduledRecordMetadataSerializer serializer() {
    return new ScheduledRecordMetadataSerializer();
  }
}