package com.rhysmccaig.kscheduler.model;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class ScheduledRecord {
  static final Logger logger = LogManager.getLogger(ScheduledRecord.class); 

  private String topic;
  private int partition;
  private long offset;
  private List<Header> headers;
  private byte[] key;
  private byte[] value;
  private ScheduledRecordMetadata metadata;

  public ScheduledRecord(ConsumerRecord<byte[], byte[]> record) {
    this(record.topic(), record.partition(), record.offset(), record.headers(), record.key(), record.value());
  };

  public ScheduledRecord(String topic, int partition, long offset, Headers headers, byte[] key, byte[] value) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.key = key;
    this.value = value;
    this.metadata = new ScheduledRecordMetadata(null, null, null, null, null, null);
    this.headers = List.of(headers.remove(ScheduledRecordMetadata.HEADER_NAME).toArray());
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  public TopicPartition topicPartition() {
    return new TopicPartition(topic, partition);
  }

  public long offset() {
    return offset;
  }

  public Headers headers() {
    var newHeaders = new RecordHeaders(headers);
    newHeaders.add(metadata.toHeader());
    return newHeaders;
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
 
}