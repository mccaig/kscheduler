package com.rhysmccaig.kschedule;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.model.ScheduledEventMetadata;
import com.rhysmccaig.kschedule.router.Router;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DelayedConsumerRunner implements Callable<Void> {

  static final Logger logger = LogManager.getLogger(DelayedConsumerRunner.class); 
  
  private static final Duration CONSUMER_POLL_DURATION = Duration.ofMillis(100);

  private final DelayedTopicConfig config;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Router router;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public DelayedConsumerRunner(Properties consumerProps, DelayedTopicConfig config, Router router) {
    // We NEVER want to auto commit offsets
    consumerProps.setProperty("enable.auto.commit", "false");
    this.consumer = new KafkaConsumer<>(consumerProps);
    this.config = config;
    this.router = router;
    logger.debug("Initialized with DelayedTopicConfig={}, Router={}", config, router);
  }

  public Void call() {
    final Map<TopicPartition, Instant> paused = new HashMap<>();
    final Map<TopicPartition, Map<Long, Future<RecordMetadata>>> awaitingCommit = new HashMap<>();
    try{
      // Rebalance listener shouldnt be required as we gracefully handle failure of pause() 
      // and resume() due to unassigned partitions?
      consumer.subscribe(List.of(config.getTopic()));
      // TODO: Add logic to gracefully shutdown
      while (!closed.get()) {
        // Get and process records
        var records = consumer.poll(CONSUMER_POLL_DURATION);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          var partition = new TopicPartition(record.topic(), record.partition());
          if (paused.containsKey(partition)) {
              // If this records partition was already paused in this batch, drop it - we will pick up later
            if (logger.isTraceEnabled()) {
              logger.trace("Skipping record from topic {}, partition {}, offset {}, as the partition is paused", record.topic(), record.partition(), record.offset());
            }
          } else {
            var kScheduleHeaders = ScheduledEventMetadata.fromHeaders(record.headers());
            if (kScheduleHeaders.getProduced() != null && kScheduleHeaders.getProduced().plus(config.getDelay()).isAfter(Instant.now())) {
            // The topic delay hasnt yet elapsed for this event, we need to pause this partition, and rewind
            try {
              consumer.pause(List.of(partition));
              paused.put(partition, kScheduleHeaders.getProduced().plus(config.getDelay()));
              consumer.seek(partition, record.offset());
            } catch (IllegalStateException ex) {
              // TODO:
              logger.warn("Attempted to pause/seek an unassigned partition: ", partition);
            }
            } else { // Otherwise we can attempt to route this message
              var result = router.route(record, kScheduleHeaders);
              awaitingCommit.putIfAbsent(partition, new HashMap<Long, Future<RecordMetadata>>());
              awaitingCommit.get(partition).put(record.offset(), result);
            }
          }
        }

        // Commit any outstanding records that have successfully been produced
        // TODO: ...
        awaitingCommit.entrySet();

        // Unpause any partitions that are have been delayed for an appropriate time
        var readyToUnpause = paused.entrySet().stream()
          .filter((entry) -> entry.getValue().isAfter(Instant.now()))
          .map(entry -> entry.getKey())
          .collect(Collectors.toSet());
        if (logger.isTraceEnabled()) {
          logger.trace("Unpausing partitions {}", readyToUnpause);
        }
        for (var partition : readyToUnpause) {
          try {
            consumer.resume(List.of(partition));
          } catch (IllegalStateException ex) {
            // Not sure if checking for assignment immediately prior to resume() would guarantee 
            // we would avoid this exception. As far as i know this should only occur if a rebalance occured
            // which should be relatively uncommon, so handing the exception shouldent incur much overhead
            logger.warn("Attempted to resume() unassigned partition: ", partition);
          } finally {
            paused.remove(partition);
          }
        }
      }
    } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) throw e;
    } finally {
      consumer.close();
    }
    return null;
  }
    

  public void shutdown() {
    closed.set(true);
    consumer.wakeup();
  }
}