package com.rhysmccaig.kscheduler.serialization;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import org.apache.kafka.common.serialization.Serde;

public class ScheduledRecordSerde implements Serde<ScheduledRecord> {
    
  private static ScheduledRecordDeserializer DESERIALIZER = new ScheduledRecordDeserializer();
  private static ScheduledRecordSerializer SERIALIZER = new ScheduledRecordSerializer();

  public ScheduledRecordDeserializer deserializer() {
    return DESERIALIZER;
  }

  public ScheduledRecordSerializer serializer() {
    return SERIALIZER;
  }
}