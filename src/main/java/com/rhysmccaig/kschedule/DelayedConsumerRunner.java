package com.rhysmccaig.kschedule;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.model.ScheduledEventMetadata;
import com.rhysmccaig.kschedule.router.Router;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DelayedConsumerRunner implements Runnable {

  static final Logger logger = LogManager.getLogger(DelayedConsumerRunner.class); 
  
  private static final Duration CONSUMER_POLL_DURATION = Duration.ofMillis(100);

  private final DelayedTopicConfig config;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Router router;
  private volatile Boolean isRunning = false;  

  public DelayedConsumerRunner(Properties consumerProps, DelayedTopicConfig config, Router router) {
    // We NEVER want to auto commit offsets
    consumerProps.setProperty("enable.auto.commit", "false");
    this.consumer = new KafkaConsumer<>(consumerProps);
    this.config = config;
    this.router = router;
    logger.debug("Initialized with DelayedTopicConfig={}, Router={}", config, router);
  }

  private synchronized Boolean getIsRunning(){
    return isRunning;
  }

  private synchronized void setIsRunning(Boolean isRnning){
    this.isRunning = isRunning;
  }  


  public void run() {
    setIsRunning(true);
    final Map<TopicPartition, Instant> paused = new HashMap<>();
    final Map<TopicPartition, Map<Long, Future<RecordMetadata>>> awaitingCommit = new HashMap<>();
    consumer.subscribe(List.of(config.getTopic()));
    // Need to add logic to gracefully shutdown
    while (getIsRunning()) {
      // Commit any outstanding records that have successfully been produced
      // TODO: ...
      awaitingCommit.entrySet();
      // Unpause any partitions that are ready to be unpaused
      // TODO: ...
      paused.entrySet();
      // Get records
      var records = consumer.poll(CONSUMER_POLL_DURATION);
      for (ConsumerRecord<byte[], byte[]> record : records) {
        var topicPartition = new TopicPartition(record.topic(), record.partition());
        if (paused.containsKey(topicPartition)) {
            // If this records partition is already paused in this batch, drop it - we will pick up later
          if (logger.isTraceEnabled()) {
            logger.trace("Skipping record from topic {}, partition {}, offset {}, as the partition is paused", record.topic(), record.partition(), record.offset());
          }
        } else {
          var kScheduleHeaders = ScheduledEventMetadata.fromHeaders(record.headers());
          if (kScheduleHeaders.getProduced() != null && kScheduleHeaders.getProduced().plus(config.getDelay()).isAfter(Instant.now())) {
          // The topic delay hasnt yet elapsed for this event, we need to pause this partition, and rewind
          paused.put(topicPartition, kScheduleHeaders.getProduced().plus(config.getDelay()));
          consumer.pause(List.of(topicPartition));
          consumer.seek(topicPartition, record.offset());
          } else { // Otherwise we can attempt to route this message
            var result = router.route(record, kScheduleHeaders);
            awaitingCommit.putIfAbsent(topicPartition, new HashMap<Long, Future<RecordMetadata>>());
            awaitingCommit.get(topicPartition).put(record.offset(), result);
          }
        }
      }
      // Commit if we can?
    }
  }
    
}