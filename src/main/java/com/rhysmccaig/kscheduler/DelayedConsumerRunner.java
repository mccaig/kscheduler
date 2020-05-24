package com.rhysmccaig.kscheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.router.Router;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DelayedConsumerRunner implements Callable<Void> {

  static final Logger logger = LogManager.getLogger(DelayedConsumerRunner.class); 
  
  private static final Duration CONSUMER_POLL_DURATION = Duration.ofMillis(100);

  private final List<String> topics;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Router router;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  public DelayedConsumerRunner(Properties consumerProps, List<String> topics, Router router) {
    // We NEVER want to auto commit offsets
    consumerProps.setProperty("enable.auto.commit", "false");
    this.consumer = new KafkaConsumer<>(consumerProps);
    this.topics = topics;
    this.router = router;
    logger.debug("Initialized with topics={}, router={}", topics, router);
  }



  public Void call() {
    try{
      final Map<TopicPartition, Instant> paused = new HashMap<>();
      final Map<TopicPartition, List<Entry<Long, Future<RecordMetadata>>>> awaitingCommit = new HashMap<>();
      consumer.subscribe(topics);
      while (!shutdown.get()) {
        // Get and process records
        var consumerRecords = consumer.poll(CONSUMER_POLL_DURATION);
        for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
          var record = new ScheduledRecord(consumerRecord);
          if (paused.containsKey(record.topicPartition())) {
              // If this records partition was already paused in this batch, drop it - we will pick up later
            if (logger.isTraceEnabled()) {
              logger.trace("Skipping record from topic {}, partition {}, offset {}, as the partition is paused", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            }
          } else {
            var delayUntil = router.delayUntil(record);
            if (delayUntil.isAfter(Instant.now())) {
              var pausePartition = record.topicPartition();
              // The topic delay hasnt yet elapsed for this event, we need to pause this partition, and rewind
              try {
                paused.put(pausePartition, delayUntil);
                consumer.pause(List.of(pausePartition));
                consumer.seek(pausePartition, record.offset());
              } catch (IllegalStateException ex) {
                paused.remove(pausePartition);
                logger.warn("Attempted to pause/seek an unassigned partition: ", pausePartition);
              }
            } else { // Otherwise we can attempt to route this message
              var result = router.route(record);
              awaitingCommit.putIfAbsent(record.topicPartition(), List.of());
              awaitingCommit.get(record.topicPartition()).add(Map.entry(consumerRecord.offset(), result));
            }
          }
        }

        // Commit any outstanding records that have successfully been produced
        // Iterate over each partition
        var awaitingIterator = awaitingCommit.entrySet().iterator();
        var toBeComitted = new HashMap<TopicPartition, OffsetAndMetadata>();
        while (awaitingIterator.hasNext()) {
          // Iterate over each awaiting offset until we find the first that isnt ready for commit
          var awaiting = awaitingIterator.next();
          var partition = awaiting.getKey();
          var offsetList = awaiting.getValue();
          var offsetIterator = offsetList.iterator();
          while (offsetIterator.hasNext()) {
            var offsetStatus = offsetIterator.next();
            var offset = offsetStatus.getKey();
            var offsetFuture = offsetStatus.getValue();
            if (!offsetFuture.isDone()) {
              break; // Found the first offset that hasnt been comitted yet.
            }
            // Will throw ExecutionException if produce failed so we can halt this thread
            // Should not throw either an InterruptedException as we only try to get the result of futures that return true for isDone()
            // Should not throw CancellationException as we dont cancel these computations
            offsetFuture.get();
            toBeComitted.put(partition, new OffsetAndMetadata(offset));
          }
          // Remove partitions from the set if there are no offsets awaiting
          if (offsetList.isEmpty()) {
            awaitingIterator.remove();
          }
        }
        if (!toBeComitted.isEmpty()) {
          consumer.commitAsync(toBeComitted, null);
        }

        // Unpause any partitions that are have been delayed for their scheduled time
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
        if (Thread.interrupted() && !shutdown.get()) {
          throw new InterruptException("Thread was interrupted when shutdown was not set");
        } 
      }
      logger.info("Stopped processing loop as shutdown flag is set");
    } catch (ExecutionException ex) {
      // This will occur if the producer encountered an issue while sending records to the broker
      // And should trigger a shutdown of the application
      logger.error("Caught ExecutionException. This should only occur if the producer failed to send() a message", ex.getCause());
    } catch (InterruptedException ex) {
      logger.error("Unexpected InterruptedException. This should not occur if the consumer has been shutdown correctly using shutdown()", ex);
    } catch (WakeupException ex) {
        // If we havent been told to shutdown, this is unexpected
        if (!shutdown.get()) {
          logger.warn("Unexpected WakeupException. This should not occur if the consumer has been shutdown correctly using shutdown()", ex);
        }
    } finally {
      logger.info("Shutting down");
      consumer.close();
    }
    return null;
  }
    

  public void shutdown() {
    logger.info("Received shutdown request");
    shutdown.set(true);
    consumer.wakeup();
  }
}