package com.rhysmccaig.kscheduler.model;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledRecord {
  static final Logger logger = LoggerFactory.getLogger(ScheduledRecord.class); 

  private ScheduledRecordMetadata metadata;
  private byte[] key;
  private byte[] value;
  private Headers headers;

  /**
   * Container object for a record that is scheduled.
   * @param metadata details about the scheduled record including scheduled time and destination
   * @param key scheduled record key
   * @param value scheduled record value
   * @param headers scheduled record headers
   */
  public ScheduledRecord(ScheduledRecordMetadata metadata, byte[] key, byte[] value, Headers headers) {
    if (metadata == null) {
      throw new NullPointerException("metadata must not be null");
    }
    this.metadata = metadata;
    this.key = key;
    this.value = value;
    this.headers = Objects.requireNonNullElse(headers, new RecordHeaders());
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
    if (o == this) {
      return true;
    } else if (!(o instanceof ScheduledRecord)) {
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
      .append((key != null) ? new String(key, UTF_8) : null)
      .append(", value=")
      .append((value != null) ? new String(value, UTF_8) : null)
      .append(", headers=")
      .append(headers)
      .append("}")
      .toString();
  }
  
}