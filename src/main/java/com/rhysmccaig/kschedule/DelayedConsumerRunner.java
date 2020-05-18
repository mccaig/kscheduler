package com.rhysmccaig.kschedule;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.model.ScheduledMessageHeaders;
import com.rhysmccaig.kschedule.strategy.SchedulerStrategy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DelayedConsumerRunner implements Runnable {

  static final Logger logger = LogManager.getLogger(DelayedConsumerRunner.class); 
  
  private static final Duration CONSUMER_POLL_DURATION = Duration.ofMillis(100);

  private final DelayedTopicConfig config;
  private final KafkaProducer<byte[], byte[]> producer;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final SchedulerStrategy strategy;
  private final ScheduledExecutorService waitScheduler;
  private Boolean running = false;

  public DelayedConsumerRunner(Properties consumerProps, DelayedTopicConfig config, SchedulerStrategy strategy, KafkaProducer<byte[], byte[]> producer) {
    // We NEVER want to auto commit offsets
    consumerProps.setProperty("enable.auto.commit", "false");
    this.consumer = new KafkaConsumer<>(consumerProps);
    this.config = config;
    this.strategy = strategy;
    this.producer = producer;
    this.waitScheduler = Executors.newScheduledThreadPool(1);
    logger.debug("Initialized with DelayedTopicConfig={}, Strategy={}", config, strategy);
  }

  public void run() {
    running = true;
    consumer.subscribe(List.of(config.getTopic()));
    // Need to add logic to gracefully shutdown
    while (running) {
      // Get records
      var records = consumer.poll(CONSUMER_POLL_DURATION);
      // We only want to process records that were added to the topic earlier than the delay
      // If the delay is negtive, then the high water mark is in the future
      var highWaterMark = config.getDelay().isNegative() ? Instant.MAX : Instant.now().minus(config.getDelay());
      for (ConsumerRecord<byte[], byte[]> record : records) {
        var kScheduleHeaders = ScheduledMessageHeaders.fromHeaders(record.headers());
        // If the headers cant be used, send the message to DLQ and commit the message
        if (kScheduleHeaders.getScheduled() == null || kScheduleHeaders.getTarget() == null) {
          logger.debug("Got message from topic {}, partition {}, offset {} that was missing kschedule headers", record.topic(), record.partition(), record.offset());
          if (strategy.getDeadLetterTopic() != null) {
            producer.send(new ProducerRecord<byte[],byte[]>(strategy.getDeadLetterTopic(), null, null, record.key(), record.value(), record.headers()));
          }
        } else {
          // Other logic
          // If record is after high watermark, pause partition, schedule unpause, seek partition for next poll
        }
      }
    }
  }
    
}