package com.rhysmccaig.kschedule.model;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import com.rhysmccaig.kschedule.router.Strategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ScheduledRecord {
  static final Logger logger = LogManager.getLogger(ScheduledRecord.class);
    
  private static String KSCHEDULE_HEADER_PREFIX = "kschedule";
  private static String KSCHEDULE_HEADER_DELIMITER = "-";
  public static String KSCHEDULE_HEADER_SCHEDULED = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "scheduled");
  public static String KSCHEDULE_HEADER_EXPIRES = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "expires");
  public static String KSCHEDULE_HEADER_DESTINATION = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "target");
  public static String KSCHEDULE_HEADER_PRODUCED = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "produced");
  public static String KSCHEDULE_HEADER_STRATEGY = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "strategy");
  public static String KSCHEDULE_HEADER_ERROR = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "error");

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
        KSCHEDULE_HEADER_SCHEDULED,
        KSCHEDULE_HEADER_EXPIRES,
        KSCHEDULE_HEADER_DESTINATION,
        KSCHEDULE_HEADER_PRODUCED,
        KSCHEDULE_HEADER_STRATEGY,
        KSCHEDULE_HEADER_ERROR
      );

      private Instant scheduled;  // Record scheduled for delivery at this time
      private Instant expires;    // Record expires at this time
      private String destination; // Destination topic for scheduled delivery
      private Instant produced;   // When the record was last produced into a topic.
      private String error;       // Any error related to the processing of this message
      private Strategy strategy;  // Routing strategy
    
      protected ScheduledRecordMetadata(Instant scheduled, Instant expires, String destination, Instant produced, Strategy strategy, String error) {
        this.scheduled = scheduled;
        this.expires = expires;
        this.destination = destination;
        this.produced = produced;
        this.strategy = strategy;
        this.error = error;
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
    
      public Instant getProduced() {
        return produced;
      }
    
      public void setProduced(Instant produced) {
        this.produced = produced;
      }
    
      public Strategy getStrategy() {
        return strategy;
      }
    
      public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
      }
    
      public String getError() {
        return error;
      }
    
      public void setError(String error) {
        this.error = error;
      }
    
      private static ScheduledRecordMetadata fromHeaders(Headers headers) {
        var scheduledHeader = headers.lastHeader(KSCHEDULE_HEADER_SCHEDULED);
        var scheduled = (scheduledHeader == null) ? null : Instant.parse(new String(scheduledHeader.value(), StandardCharsets.UTF_8));
        var expiresHeader = headers.lastHeader(KSCHEDULE_HEADER_EXPIRES);
        var expires = (expiresHeader == null) ? null : Instant.parse(new String(expiresHeader.value(), StandardCharsets.UTF_8));
        var targetHeader = headers.lastHeader(KSCHEDULE_HEADER_DESTINATION);
        var target = (targetHeader == null) ? null : new String(targetHeader.value(), StandardCharsets.UTF_8);
        var producedHeader = headers.lastHeader(KSCHEDULE_HEADER_PRODUCED);
        var produced = (producedHeader == null) ? null : Instant.parse(new String(producedHeader.value(), StandardCharsets.UTF_8));
        var strategyHeader = headers.lastHeader(KSCHEDULE_HEADER_STRATEGY);
        Strategy strategy = null;
        if (strategyHeader != null) {
          var strategyName = new String(strategyHeader.value(), StandardCharsets.UTF_8);
          try {
            strategy = Strategy.valueOf(strategyName);
          } catch (IllegalArgumentException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Scheduled event metadata specificed an invalid routing strategy: {}", strategyName);
            }
          }
        }
        var errorHeader = headers.lastHeader(KSCHEDULE_HEADER_ERROR);
        var error = (errorHeader == null) ? null : new String(errorHeader.value(), StandardCharsets.UTF_8);
        return new ScheduledRecordMetadata(scheduled, expires, target, produced, strategy, error);
      }

      // String encoding of instants is nice and readable, but maybe hurts performance too much
      // TODO: Consider optimization by using long or similar encoding for Instant type
      public List<Header> toHeaders() {
        List<Header> headers = List.of();
        if (scheduled != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_SCHEDULED, scheduled.toString().getBytes(StandardCharsets.UTF_8)));
        }
        if (expires != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_EXPIRES, expires.toString().getBytes(StandardCharsets.UTF_8)));
        }
        if (destination != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_DESTINATION, destination.getBytes(StandardCharsets.UTF_8)));
        }
        if (produced != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_PRODUCED, produced.toString().getBytes(StandardCharsets.UTF_8)));
        }
        if (strategy != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_STRATEGY, strategy.name().getBytes(StandardCharsets.UTF_8)));
        }
        if (error != null) {
          headers.add(new RecordHeader(KSCHEDULE_HEADER_ERROR, error.getBytes(StandardCharsets.UTF_8)));
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