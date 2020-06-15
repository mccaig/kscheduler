package com.rhysmccaig.kscheduler.serialization;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;

import org.apache.kafka.common.serialization.Serde;

public class ScheduledRecordSerde implements Serde<ScheduledRecord> {
    
  public ScheduledRecordDeserializer deserializer() {
    return new ScheduledRecordDeserializer();
  }

  public ScheduledRecordSerializer serializer() {
    return new ScheduledRecordSerializer();
  }
}