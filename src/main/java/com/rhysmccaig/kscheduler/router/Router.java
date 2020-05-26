package com.rhysmccaig.kscheduler.router;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.TopicPartitionOffset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Routes Scheduled Records to the next hop
 * Thread Safe.
 */
public class Router {
  static final Logger logger = LogManager.getLogger(Router.class); 

  public static final RoutingStrategy DEFAULT_ROUTING_STRATEGY = new NotBeforeStrategy();

  protected final SortedSet<DelayedTopicConfig> delayedTopicsSet;
  protected final Map<String, Duration> topicDelays;
  protected final String deadLetterTopic;
  protected final RoutingStrategy routingStrategy;
  protected final KafkaProducer<ScheduledRecordMetadata, ScheduledRecord> producer;

  public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, KafkaProducer<ScheduledRecordMetadata, ScheduledRecord> producer, RoutingStrategy defaultRoutingStrategy) {
    this.delayedTopicsSet = delayedTopics.stream()
        .collect(Collectors.toCollection(() -> 
            new TreeSet<DelayedTopicConfig>((a, b) -> a.getDelay().compareTo(b.getDelay()))));
    this.topicDelays = delayedTopics.stream()
        .collect(Collectors.toMap(DelayedTopicConfig::getName, DelayedTopicConfig::getDelay));
    this.deadLetterTopic = deadLetterTopic;
    this.routingStrategy = (defaultRoutingStrategy == null) ? new NotBeforeStrategy() : defaultRoutingStrategy;
    this.producer = producer;
  }

  public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, KafkaProducer<ScheduledRecordMetadata, ScheduledRecord>  producer) {
    this(delayedTopics, deadLetterTopic, producer, DEFAULT_ROUTING_STRATEGY);
  }

  public Future<RecordMetadata> forward(ConsumerRecord<ScheduledRecordMetadata, ScheduledRecord> crecord) {
    return forward(crecord, Instant.now());
  }

  public Future<RecordMetadata> forward(ConsumerRecord<ScheduledRecordMetadata, ScheduledRecord> crecord, Instant now) {
    final Future<RecordMetadata> produceResult;
    final var metadata = crecord.key();
    final var record = crecord.value();
    final var source = TopicPartitionOffset.fromConsumerRecord(crecord);
    if (metadata.expires() != null && metadata.expires().isBefore(now)) {
      var reason = new StringBuilder()
          .append("expired record: ")
          .append(source)
          .toString();
      produceResult = drop(record, reason, now);
    } else {
      // All clear, continue to route the event
      var nextTopic = routingStrategy.nextTopic(delayedTopicsSet, metadata, now);
      produceResult = send(record, nextTopic, now);
    }
    return produceResult;
  }

  public Future<RecordMetadata> drop(ScheduledRecord record, String reason) {
    return drop(record, reason, Instant.now());
  }

  public Future<RecordMetadata> drop(ScheduledRecord record, String reason, Instant now) {
    logger.info("dropping record: {}", reason);
    if (deadLetterTopic != null) {
      record.metadata().setError(reason);
      return send(record, deadLetterTopic, now);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  public Future<RecordMetadata> send(ScheduledRecord record, String destination, Instant now) {
    record.metadata().setProduced(now);
    var key = record.metadata();
    var value = record;
    return producer.send(new ProducerRecord<ScheduledRecordMetadata, ScheduledRecord>(destination, null, null, key, value));
  }

  public Instant processAt(ConsumerRecord<ScheduledRecordMetadata, ScheduledRecord> crecord) {
    final var metadata = crecord.key();
    final var source = TopicPartitionOffset.fromConsumerRecord(crecord);
    final var delay = topicDelays.get(source.topic());
    final Instant delayUntil;
    if ((delay == null) || metadata.produced() == null) {
      delayUntil = Instant.MIN;
    } else {
      delayUntil = metadata.produced().plus(delay);
    }
    return delayUntil;
  }

}