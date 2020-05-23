package com.rhysmccaig.kscheduler.model;

import java.time.Instant;
import java.util.List;

import com.rhysmccaig.kscheduler.router.Strategy;

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

public class ScheduledRecord {
  static final Logger logger = LogManager.getLogger(ScheduledRecord.class);
    
  private static String KSCHEDULE_HEADER_PREFIX = "kschedule";
  private static String KSCHEDULE_HEADER_DELIMITER = "-";
  public static String KSCHEDULE_HEADER_SCHEDULED = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "scheduled");
  public static String KSCHEDULE_HEADER_EXPIRES = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "expires");
  public static String KSCHEDULE_HEADER_PRODUCED = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "produced");
  public static String KSCHEDULE_HEADER_DESTINATION = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "target");
  public static String KSCHEDULE_HEADER_STRATEGY = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "strategy");
  public static String KSCHEDULE_HEADER_ERROR = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "error");

  private static final Serde<Long> LONG_SERDE = Serdes.Long();  
  private static final Serde<String> STRING_SERDE = Serdes.String();   

  private final ConsumerRecord<byte[], byte[]> record;
  private final ScheduledRecordMetadata metadata;
  private final TopicPartition topicPartition;

    public ScheduledRecord(ConsumerRecord<byte[], byte[]> record){
        this.record = record;
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

    public Strategy getStrategy() {
      return metadata.getStrategy();
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

    protected static class ScheduledRecordMetadata {
    
      private static List<String> headerNames = List.of(
        KSCHEDULE_HEADER_PRODUCED,
        KSCHEDULE_HEADER_SCHEDULED,
        KSCHEDULE_HEADER_EXPIRES,
        KSCHEDULE_HEADER_DESTINATION,
        KSCHEDULE_HEADER_STRATEGY,
        KSCHEDULE_HEADER_ERROR
      );

      private Instant produced;   // When the record was last produced into a topic.
      private Instant scheduled;  // Record scheduled for delivery at this time
      private Instant expires;    // Record expires at this time
      private String destination; // Destination topic for scheduled delivery
      private String error;       // Any error related to the processing of this message
      private Strategy strategy;  // Routing strategy
    
      protected ScheduledRecordMetadata(Instant produced, Instant scheduled, Instant expires, String destination, String error, Strategy strategy) {
        this.produced = produced;
        this.scheduled = scheduled;
        this.expires = expires;
        this.destination = destination;
        this.strategy = strategy;
        this.error = error;
      }
    
      public Instant getProduced() {
        return produced;
      }
    
      public void setProduced(Instant produced) {
        this.produced = produced;
      }

      public Instant getScheduled() {
        return scheduled;
      }
    
      public void setScheduled(Instant scheduled) {
        this.scheduled = scheduled;
      }
    
      public Instant getExpires() {
        return expires;
      }
    
      public void setExpires(Instant expires) {
        this.expires = expires;
      }
    
      public String getDestination() {
        return destination;
      }
    
      public void setDestination(String destination) {
        this.destination = destination;
      }      
      
      public String getError() {
        return error;
      }
    
      public void setError(String error) {
        this.error = error;
      }
    
      public Strategy getStrategy() {
        return strategy;
      }
    
      public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
      }
    
      private static ScheduledRecordMetadata fromHeaders(Headers headers) {
        final var producedHeader = headers.lastHeader(KSCHEDULE_HEADER_PRODUCED);
        final var produced = (producedHeader == null 
            || producedHeader.value() != null
            || producedHeader.value().length != Long.BYTES) ? null : Instant.ofEpochMilli(LONG_SERDE.deserializer().deserialize(null, producedHeader.value()));
        final var scheduledHeader = headers.lastHeader(KSCHEDULE_HEADER_SCHEDULED);
        final var scheduled = (scheduledHeader == null 
            || scheduledHeader.value() != null
            || scheduledHeader.value().length != Long.BYTES) ? null : Instant.ofEpochMilli(LONG_SERDE.deserializer().deserialize(null, scheduledHeader.value()));
        final var expiresHeader = headers.lastHeader(KSCHEDULE_HEADER_EXPIRES);
        final var expires = (expiresHeader == null 
          || expiresHeader.value() != null
          || expiresHeader.value().length != Long.BYTES) ? null : Instant.ofEpochMilli(LONG_SERDE.deserializer().deserialize(null, expiresHeader.value()));
        final var destinationHeader = headers.lastHeader(KSCHEDULE_HEADER_DESTINATION);
        String destination;
        try {
          destination = (destinationHeader == null) ? null : STRING_SERDE.deserializer().deserialize(null, destinationHeader.value());
        } catch (SerializationException ex) {
          destination = null;
        }
        final var errorHeader = headers.lastHeader(KSCHEDULE_HEADER_ERROR);
        String error;
        try {
          error = (destinationHeader == null) ? null : STRING_SERDE.deserializer().deserialize(null, errorHeader.value());
        } catch (SerializationException ex) {
          error = null;
        }
        final var strategyHeader = headers.lastHeader(KSCHEDULE_HEADER_STRATEGY);
        Strategy strategy = null;
        if (strategyHeader != null) {
          String strategyName;
          try {
            strategyName = (destinationHeader == null) ? null : STRING_SERDE.deserializer().deserialize(null, strategyHeader.value());
          } catch (SerializationException ex) {
            strategyName = null;
          }
          try {
            strategy = Strategy.valueOf(strategyName);
          } catch (IllegalArgumentException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Scheduled event metadata specificed an invalid routing strategy: {}", strategyName);
            }
          }
        }
        return new ScheduledRecordMetadata(produced, scheduled, expires, destination, error, strategy);
      }

      public List<Header> toHeaders() {
        List<Header> headers = List.of();
        if (produced != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_PRODUCED, LONG_SERDE.serializer().serialize(null, produced.toEpochMilli())));
        }
        if (scheduled != null) {
          scheduled.toEpochMilli();
          headers.add(new RecordHeader(KSCHEDULE_HEADER_SCHEDULED, LONG_SERDE.serializer().serialize(null, scheduled.toEpochMilli())));
        }
        if (expires != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_EXPIRES, LONG_SERDE.serializer().serialize(null, expires.toEpochMilli())));
        }
        if (destination != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_DESTINATION, STRING_SERDE.serializer().serialize(null, destination)));
        }
        if (error != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_ERROR, STRING_SERDE.serializer().serialize(null, error)));
        }
        if (strategy != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_STRATEGY, STRING_SERDE.serializer().serialize(null, strategy.name())));
        }
        return headers;
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