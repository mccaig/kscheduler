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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO: Validate thread-safety of this class
public class Router {
  static final Logger logger = LogManager.getLogger(Router.class); 

  protected final SortedSet<DelayedTopicConfig> delayedTopicsSet;
  protected final Map<String, Duration> topicDelays;
  protected final String deadLetterTopic;
  protected final RoutingStrategy defaultRouterStrategy;
  protected final KafkaProducer<byte[], byte[]> producer;

  public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, RoutingStrategy defaultRoutingStrategy, KafkaProducer<byte[], byte[]> producer) {
    this.delayedTopicsSet = delayedTopics.stream()
        .collect(Collectors.toCollection(() -> 
            new TreeSet<DelayedTopicConfig>((a, b) -> a.getDelay().compareTo(b.getDelay()))));
    this.topicDelays = delayedTopics.stream()
        .collect(Collectors.toMap(DelayedTopicConfig::getName, DelayedTopicConfig::getDelay));
    this.deadLetterTopic = deadLetterTopic;
    this.defaultRouterStrategy = defaultRoutingStrategy;
    this.producer = producer;
  }

  public Future<RecordMetadata> route(ScheduledRecord record) {
    return route(record, Instant.now());
  }

  public Future<RecordMetadata> route(ScheduledRecord record, Instant now) {
    final Future<RecordMetadata> produceResult;
    if (record.getDestination() == null) {
      // If the destination header isnt set, then we dont know where to route the record to
      logger.warn("Unable to route message from topic {}, partition {}, offset {} that was missing kschedule destination header", record.getTopic(), record.getPartition(), record.getOffset());
      record.setError("Missing destination topic in record.");
      produceResult = routeDeadLetter(record);
    } else if (record.getScheduled() == null) {
      // If the record has no scheduled time, then drop the message, we dont want to assume when it should be delivered
      logger.warn("Unable to route message from topic {}, partition {}, offset {} that was missing kschedule destination header", record.getTopic(), record.getPartition(), record.getOffset());
      record.setError("Missing destination topic in record.");
      produceResult = routeDeadLetter(record);
    } else if (record.hasExpired(now)) {
      if (logger.isDebugEnabled()) {
        logger.debug("message from topic {}, partition {}, offset {} expired at {}", record.getTopic(), record.getPartition(), record.getOffset(), record.getExpires().toString());
      }
      record.setError("Record Expired.");
      produceResult = routeDeadLetter(record);
    } else {
      // All clear, continue to route the event
      var routingStrategy = (record.getStrategy() == null) ? defaultRouterStrategy : record.getStrategy();
      var nextTopic = routingStrategy.getNextTopic(delayedTopicsSet, now, record.getScheduled(), record.getDestination());
      produceResult = producer.send(new ProducerRecord<byte[],byte[]>(nextTopic, null, null, record.getKey(), record.getValue(), record.getHeaders()));
    }
    return produceResult;
  }

  public Future<RecordMetadata> routeDeadLetter(ScheduledRecord record) {
    if (deadLetterTopic != null) {
      return producer.send(new ProducerRecord<byte[],byte[]>(deadLetterTopic, null, null, record.getKey(), record.getValue(), record.getHeaders()));
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  public Instant delayUntil(ScheduledRecord record) {
    final var topicDelay = topicDelays.get(record.getTopic());
    final Instant delayUntil;
    if ((topicDelay == null) || record.getProduced() == null) {
      delayUntil = Instant.MIN;
    } else {
      delayUntil = record.getProduced().plus(topicDelay);
    }
    return delayUntil;
  }

}