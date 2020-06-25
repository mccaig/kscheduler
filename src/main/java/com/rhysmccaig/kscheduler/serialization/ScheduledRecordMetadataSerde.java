package com.rhysmccaig.kscheduler.serialization;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import org.apache.kafka.common.serialization.Serde;

public class ScheduledRecordMetadataSerde implements Serde<ScheduledRecordMetadata> {
  
  private static ScheduledRecordMetadataDeserializer DESERIALIZER = new ScheduledRecordMetadataDeserializer();
  private static ScheduledRecordMetadataSerializer SERIALIZER = new ScheduledRecordMetadataSerializer();

  public ScheduledRecordMetadataDeserializer deserializer() {
    return DESERIALIZER;
  }

  public ScheduledRecordMetadataSerializer serializer() {
    return SERIALIZER;
  }
}