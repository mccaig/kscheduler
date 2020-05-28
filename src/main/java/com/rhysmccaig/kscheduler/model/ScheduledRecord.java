package com.rhysmccaig.kscheduler.model;


import org.apache.kafka.common.header.Headers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ScheduledRecord {
  static final Logger logger = LogManager.getLogger(ScheduledRecord.class); 

  private ScheduledRecordMetadata metadata;
  private byte[] key;
  private byte[] value;
  private Headers headers;

  public ScheduledRecord(ScheduledRecordMetadata metadata, byte[] key, byte[] value, Headers headers) {
    if (metadata == null) {
      throw new NullPointerException("metadata must not be null");
    }
    this.metadata = metadata;
    this.key = key;
    this.value = value;
    this.headers = headers;
  }

  public byte[] key() {
    return key;
  }

  public byte[] value() {
    return value;
  }

  public ScheduledRecordMetadata metadata() {
    return metadata;
  }

  public Headers headers() {
    return headers;
  }
  
}