package com.rhysmccaig.kschedule;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.model.ScheduledMessageHeaders;
import com.rhysmccaig.kschedule.router.Router;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DelayedConsumerRunner implements Runnable {

  static final Logger logger = LogManager.getLogger(DelayedConsumerRunner.class); 
  
  private static final Duration CONSUMER_POLL_DURATION = Duration.ofMillis(100);

  private final DelayedTopicConfig config;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Router router;
  private final ScheduledExecutorService waitScheduler;
  private Boolean running = false;

  public DelayedConsumerRunner(Properties consumerProps, DelayedTopicConfig config, Router router) {
    // We NEVER want to auto commit offsets
    consumerProps.setProperty("enable.auto.commit", "false");
    this.consumer = new KafkaConsumer<>(consumerProps);
    this.config = config;
    this.router = router;
    this.waitScheduler = Executors.newScheduledThreadPool(1);
    logger.debug("Initialized with DelayedTopicConfig={}, Router={}", config, router);
  }

  public void run() {
    running = true;
    consumer.subscribe(List.of(config.getTopic()));
    // Need to add logic to gracefully shutdown
    while (running) {
      // Get records
      var records = consumer.poll(CONSUMER_POLL_DURATION);
      List<Integer> pausedPartitions = List.of();
      for (ConsumerRecord<byte[], byte[]> record : records) {
        var kScheduleHeaders = ScheduledMessageHeaders.fromHeaders(record.headers());
        if (pausedPartitions.contains(record.partition())) {
            // If this records partition is already paused in this batch, drop it - we will pick up later
          if (logger.isTraceEnabled()) {
            logger.trace("Skipping record from topic {}, partition {}, offset {}, as the partition is paused", record.topic(), record.partition(), record.offset());
          }
        } else if (kScheduleHeaders.getProduced() == null || kScheduleHeaders.getScheduled() == null || kScheduleHeaders.getTarget() == null) {
          // If the headers cant be used, send the message to DLQ and commit the message
          logger.debug("Got message from topic {}, partition {}, offset {} that was missing kschedule headers", record.topic(), record.partition(), record.offset());
          kScheduleHeaders.setError(new StringBuilder(128)
              .append("Missing headers in record received from ")
              .append(record.topic())
              .append(":")
              .append(record.topic())
              .append(":")
              .append(record.topic())
              .toString());
          var result = router.routeDeadLetter(record.key(), record.value(), kScheduleHeaders.merge(record.headers()));
          // Need to consume future to determine whether to commit?
          consumer.commitAsync(offsets, callback);
        } else if (kScheduleHeaders.getProduced().plus(config.getDelay()).isAfter(Instant.now())) {
            // We only want to process records that were added to the topic earlier than the delay
            // If record is after high watermark, pause partition, schedule unpause, seek partition for next poll
            // this topic partition needs to be paused - figure out for how long and seek accordingly
        } else {
          // Lets process that message
        }
      }
    }
  }
    
}