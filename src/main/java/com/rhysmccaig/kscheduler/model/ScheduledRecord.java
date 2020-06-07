package com.rhysmccaig.kscheduler.model;


import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
    this.headers = Objects.nonNull(headers) ? headers : new RecordHeaders();
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

  @Override
  public int hashCode() {
      return Objects.hash(metadata, key, value, headers);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ScheduledRecord)) {
        return false;
    }
    var co = (ScheduledRecord) o;
    return Objects.equals(metadata, co.metadata)
        && Arrays.equals(key, co.key)
        && Arrays.equals(value, co.value) 
        && Objects.equals(headers, co.headers);
  }

  @Override
  public String toString() {
    return new StringBuilder().append(ScheduledRecord.class.getSimpleName())
      .append("{metadata=")
      .append(metadata)
      .append(", key=")
      .append(Objects.nonNull(key) ? new String(key, StandardCharsets.UTF_8) : null)
      .append(", value=")
      .append(Objects.nonNull(value) ? new String(value, StandardCharsets.UTF_8) : null)
      .append(", headers=")
      .append(headers)
      .append("}")
      .toString();
  }
  
}