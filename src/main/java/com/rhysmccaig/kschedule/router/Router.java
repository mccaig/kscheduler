package com.rhysmccaig.kschedule.router;

import java.time.Instant;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.model.ScheduledEventMetadata;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Router {
  static final Logger logger = LogManager.getLogger(Router.class); 

  protected final SortedSet<DelayedTopicConfig> delayedTopicsSet;
  protected final String deadLetterTopic;
  protected final RoutingStrategy defaultRouterStrategy;
  protected final KafkaProducer<byte[], byte[]> producer;

  public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, RoutingStrategy defaultRoutingStrategy, KafkaProducer<byte[], byte[]> producer) {
    this.delayedTopicsSet = delayedTopics.stream()
        .collect(Collectors.toCollection(() -> 
            new TreeSet<DelayedTopicConfig>((a, b) -> a.getDelay().compareTo(b.getDelay()))));
    this.deadLetterTopic = deadLetterTopic;
    this.defaultRouterStrategy = defaultRoutingStrategy;
    this.producer = producer;
  }

  public Future<RecordMetadata> route(ConsumerRecord<byte[], byte[]> record, ScheduledEventMetadata kScheduleHeaders) {
    final Future<RecordMetadata> produceResult;
    var now = Instant.now();
    if (kScheduleHeaders.getProduced() == null || kScheduleHeaders.getScheduled() == null || kScheduleHeaders.getTarget() == null) {
      // If the mandatory headers dont exist we dont know how to route the message, other than to the DLQ
      logger.debug("Unable to route message from topic {}, partition {}, offset {} that was missing kschedule headers", record.topic(), record.partition(), record.offset());
      kScheduleHeaders.setError(new StringBuilder(128)
          .append("Missing headers in record received from ")
          .append(record.topic())
          .append(":")
          .append(record.partition())
          .append(":")
          .append(record.offset())
          .toString());
      produceResult = routeDeadLetter(record, kScheduleHeaders);
    } else if (kScheduleHeaders.getExpires() != null && kScheduleHeaders.getExpires().isBefore(now)) {
      if (logger.isDebugEnabled()) {
        logger.debug("message from topic {}, partition {}, offset {} expired at {}", record.topic(), record.partition(), record.offset(), kScheduleHeaders.getExpires().toString());
      }
      kScheduleHeaders.setError(new StringBuilder(128)
          .append("Missing headers in record received from ")
          .append(record.topic())
          .append(":")
          .append(record.partition())
          .append(":")
          .append(record.offset())
          .toString());
      produceResult = routeDeadLetter(record, kScheduleHeaders);
    } else {
      // All clear, continue to route the event
      var routingStrategy = (kScheduleHeaders.getStrategy() == null) ? defaultRouterStrategy : kScheduleHeaders.getStrategy();
      var nextTopic = routingStrategy.getNextTopic(delayedTopicsSet, now, kScheduleHeaders.getScheduled(), kScheduleHeaders.getTarget());
      produceResult = producer.send(new ProducerRecord<byte[],byte[]>(nextTopic, null, null, record.key(), record.value(), kScheduleHeaders.merge(record.headers())));
    }
    return produceResult;
  }

  
  public Future<RecordMetadata> routeDeadLetter(ConsumerRecord<byte[], byte[]> record, ScheduledEventMetadata kScheduleHeaders) {
    if (deadLetterTopic != null) {
      return producer.send(new ProducerRecord<byte[],byte[]>(deadLetterTopic, null, null, record.key(), record.value(), kScheduleHeaders.merge(record.headers())));
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  

}