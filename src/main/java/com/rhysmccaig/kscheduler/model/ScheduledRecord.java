package com.rhysmccaig.kscheduler.model;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import com.esotericsoftware.kryo.Kryo;

public class ScheduledRecord {
  static final Logger logger = LogManager.getLogger(ScheduledRecord.class);
    
  private static String METADATA_HEADER_NAME = "kscheduler-record";

  private static final Serde<Long> LONG_SERDE = Serdes.Long();  
  private static final Serde<String> STRING_SERDE = Serdes.String();   

  private final TopicPartition topicPartition;
  private final int partition;
  private final long offset;
  private final long timestamp;
  private final Headers headers;
  private final byte[] key;
  private final byte[] value;
  private final ScheduledRecordMetadata metadata;

  private static ScheduledRecordMetadata extractMetadata(Headers headers) {
    if (headers.lastHeader(METADATA_HEADER_NAME) == null) {
      return null;
    } else {
      //Kryo

    }
  }

    public ScheduledRecord(ConsumerRecord<byte[], byte[]> record){
    this.topicPartition = new TopicPartition(record.topic(), record.partition());
    this.partition = record.partition();
    this.offset = record.offset();
    this.headers = record.headers();
    this.key = record.key();
    this.value = record.value();
    this.metadata = record.headers()

        this.metadata = ScheduledRecordMetadata.fromHeaders(record.headers());
        this.topicPartition = new TopicPartition(record.topic(), record.partition());
    };

    public boolean hasExpired() {
        return hasExpired(Instant.now());
    }

    public boolean hasExpired(Instant moment) {
        if (metadata.getExpires() == null) {
            return false;
        } else {
            return metadata.getExpires().isBefore(moment);
        }
    }

    public Instant getExpires() {
      return metadata.getExpires();
    }

    public Instant getProduced() {
      return metadata.getProduced();
    }

    public Instant getScheduled() {
      return metadata.getScheduled();
    }

    public String getDestination() {
      return metadata.getDestination();
    }

    public String getTopic() {
      return record.topic();
    }

    public int getPartition() {
      return record.partition();
    }

    public TopicPartition getTopicPartition() {
      return topicPartition;
    }

    public long getOffset() {
      return record.offset();
    }

    public byte[] getKey() {
      return record.key();
    }

    public byte[] getValue() {
      return record.value();
    }

    public List<Header> getHeaders() {
      return metadata.mergeHeaders(record.headers());
    }

    public void setError(String message) {
      metadata.setError(new StringBuilder(128)
          .append(message)
          .append(". (")
          .append(getTopic())
          .append(":")
          .append(getPartition())
          .append("-")
          .append(getOffset())
          .append(")")
          .toString());
    }
    
      /**
       * @param headers existing headers object copy headers from
       * @return new headers object with kschedule headers added or updated
       */
      public List<Header> mergeHeaders(Iterable<Header> headers) {
        List<Header> newHeaders = this.toHeaders();
        for (var header : headers) {
          if (!headerNames.contains(header.key())) { // Copy existing headers that arent kschedule headers
            newHeaders.add(header);
          }
        }
        return newHeaders;
      }
    
    }

}