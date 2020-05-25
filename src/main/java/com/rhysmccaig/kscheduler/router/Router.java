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
import com.rhysmccaig.kscheduler.model.TopicPartitionOffset;

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

  protected final SortedSet<DelayedTopicConfig> delayedTopicsSet;
  protected final Map<String, Duration> topicDelays;
  protected final String deadLetterTopic;
  protected final RoutingStrategy routingStrategy;
  protected final KafkaProducer<byte[], byte[]> producer;

  public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, KafkaProducer<byte[], byte[]> producer, RoutingStrategy defaultRoutingStrategy) {
    this.delayedTopicsSet = delayedTopics.stream()
        .collect(Collectors.toCollection(() -> 
            new TreeSet<DelayedTopicConfig>((a, b) -> a.getDelay().compareTo(b.getDelay()))));
    this.topicDelays = delayedTopics.stream()
        .collect(Collectors.toMap(DelayedTopicConfig::getName, DelayedTopicConfig::getDelay));
    this.deadLetterTopic = deadLetterTopic;
    this.routingStrategy = (defaultRoutingStrategy == null) ? new NotBeforeStrategy() : defaultRoutingStrategy;
    this.producer = producer;
  }

  public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, KafkaProducer<byte[], byte[]> producer) {
    this(delayedTopics, deadLetterTopic, producer, null);
  }

  public Future<RecordMetadata> route(TopicPartitionOffset source, ScheduledRecord record) {
    return route(source, record, Instant.now());
  }

  public Future<RecordMetadata> route(TopicPartitionOffset source, ScheduledRecord record, Instant now) {
    final Future<RecordMetadata> produceResult;
    if (record.metadata().getDestination() == null) {
      // If the destination header isnt set, then we dont know where to route the record to
      logger.warn("Unable to route message from {}. (missing kscheduler destination header)", source);
      produceResult = routeDeadLetter(record, now);
      record.metadata().setError("missing destination header: " + source + "@" + now);
    } else if (record.metadata().getScheduled() == null) {
      // If the record has no scheduled time, then drop the message, we dont want to assume when it should be delivered
      logger.warn("Unable to route message from {}. (missing kscheduler scheduled header)", source);
      record.metadata().setError("missing scheduled header: " + source + "@" + now);
      produceResult = routeDeadLetter(record, now);
    } else if (record.metadata().getExpires() != null && record.metadata().getExpires().isBefore(now)) {
      if (logger.isDebugEnabled()) {
        logger.debug("message from {} expired at {}", source, record.metadata().getExpires().toString());
      }
      record.metadata().setError("expired record: " + source + "@" + now);
      produceResult = routeDeadLetter(record, now);
    } else {
      // All clear, continue to route the event
      var nextTopic = routingStrategy.getNextTopic(delayedTopicsSet, now, record.metadata().getScheduled(), record.metadata().getDestination());
      produceResult = send(record, nextTopic, now);
    }
    return produceResult;
  }

  private Future<RecordMetadata> send(ScheduledRecord record, String destination, Instant now) {
    record.metadata().setProduced(now);
    return producer.send(new ProducerRecord<byte[],byte[]>(destination, null, null, record.key(), record.value(), record.headers()));
  }

  public Future<RecordMetadata> routeDeadLetter(ScheduledRecord record) {
    return routeDeadLetter(record, Instant.now());
  }

  public Future<RecordMetadata> routeDeadLetter(ScheduledRecord record, Instant now) {
    if (deadLetterTopic != null) {
      return send(record, deadLetterTopic, now);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  public Instant processAt(TopicPartitionOffset source, ScheduledRecord record) {
    final var topicDelay = topicDelays.get(source.getTopic());
    final Instant delayUntil;
    if ((topicDelay == null) || record.metadata().getProduced() == null) {
      delayUntil = Instant.MIN;
    } else {
      delayUntil = record.metadata().getProduced().plus(topicDelay);
    }
    return delayUntil;
  }

}