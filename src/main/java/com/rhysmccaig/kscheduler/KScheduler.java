package com.rhysmccaig.kscheduler;

import com.typesafe.config.ConfigFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.xml.crypto.dsig.Transform;

import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;
import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.streams.ScheduleProcessor;
import com.rhysmccaig.kscheduler.streams.SourceKeyDefaultStreamPartitioner;
import com.rhysmccaig.kscheduler.streams.SourceToScheduledTransformer;
import com.rhysmccaig.kscheduler.streams.ToOriginalRecordProcessor;
import com.rhysmccaig.kscheduler.router.NotBeforeStrategy;
import com.rhysmccaig.kscheduler.router.Router;
import com.rhysmccaig.kscheduler.router.RoutingStrategy;
import com.rhysmccaig.kscheduler.serdes.ScheduledIdSerde;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataSerde;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordSerde;
import com.rhysmccaig.kscheduler.util.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;


public class KScheduler {
  static final Logger logger = LogManager.getLogger(KScheduler.class); 

  public static void main(String[] args) {
    final Config config = ConfigFactory.load();
    final Config schedulerConfig = config.getConfig("scheduler");
    final Config topicsConfig = config.getConfig("topics");
    final Config kafkaConfig = config.getConfig("kafka");

    final Integer consumerThreads = schedulerConfig.getInt("consumer.threads");
    final Duration consumerShutdownTimeout = schedulerConfig.getDuration("consumer.shutdown.timeout");
    final Duration producerShutdownTimeout = schedulerConfig.getDuration("producer.shutdown.timeout");
    final Duration streamsShutdownTimeout = schedulerConfig.getDuration("streams.shutdown.timeout");
;
Serdes.ByteArray().getClass().getName();
    Properties producerProps = ConfigUtils.toProperties(kafkaConfig.withFallback(kafkaConfig.getConfig("producer")));
    Properties consumerProps = ConfigUtils.toProperties(kafkaConfig.withFallback(kafkaConfig.getConfig("consumer")));
    Properties streamsProps = ConfigUtils.toProperties(kafkaConfig.withFallback(kafkaConfig.getConfig("streams")));

    var delayedTopicsConfig = topicsConfig.getConfig("delayed");
    var delayedTopicsNames = topicsConfig.getObject("delayed").keySet();
    List<DelayedTopicConfig> delayedTopics = delayedTopicsNames.stream().map(name -> {
      var delayedTopicConfig = delayedTopicsConfig.getConfig(name);
      var delay = delayedTopicConfig.getDuration("delay");
      var topic = delayedTopicConfig.hasPath("topic") ? delayedTopicConfig.getString("topic") : name;
      return new DelayedTopicConfig(name, topic, delay);
    }).collect(Collectors.toList());
    
    final var scheduledTopicConfig = new DelayedTopicConfig("scheduled", config.getString("topics.scheduled"), Duration.ofSeconds(Long.MIN_VALUE));
    delayedTopics.add(scheduledTopicConfig);
    
    final var dlqTopic = config.getIsNull("topics.dlq") ? null : config.getString("topics.dlq");
    
    // Set up the producer
    final var producer = new KafkaProducer<byte[], byte[]>(producerProps);

    // Set up a topic router
    final Config routerConfig = schedulerConfig.getConfig("router");
    final RoutingStrategy defaultRouterStrategy = new NotBeforeStrategy(routerConfig.getDuration("delay.grace.period"));
    final var topicRouter = new Router(delayedTopics, dlqTopic, producer, defaultRouterStrategy);
    // Set up a consumers
    // One consumer thread per input topic for now
    final var topics = delayedTopics.stream()
        .map(dt -> dt.getTopic())
        .collect(Collectors.toList());
    final var maximumScheduledDelay = schedulerConfig.getDuration("maximum.delay");
    // Construct consumer runners - one for each desired thread
    final var consumerRunners = new ArrayList<DelayedConsumerRunner>(consumerThreads);
    for (var i = 0; i < consumerThreads; i++) {
      consumerRunners.add(new DelayedConsumerRunner(consumerProps, topics, topicRouter, maximumScheduledDelay));
    }
    final var consumerExecutorService = Executors.newFixedThreadPool(consumerThreads);
    final CompletionService<Void> consumerEcs = new ExecutorCompletionService<>(consumerExecutorService);


    // TODO: Make names configurable
    StoreBuilder<KeyValueStore<ScheduledId, ScheduledRecord>> storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(ScheduleProcessor.STATE_STORE_NAME),
        new ScheduledIdSerde(),
        new ScheduledRecordSerde())
      .withLoggingEnabled(Collections.emptyMap());
    
    // Streams component

    // If there are delayed topics, send input to scheduled topic, else send straight to scheduler
    // TODO: Implement logic
    var scheduledTopic = topicsConfig.getString("scheduled");
    var builder = new StreamsBuilder();
    builder.stream(topicsConfig.getString("input"), Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
        .transform(() -> new SourceToScheduledTransformer(), Named.as("SOURCE_TO_SCHEDULED"))
        .to(scheduledTopic, Produced.with(new ScheduledRecordMetadataSerde(), new ScheduledRecordSerde()));
      
    final Topology topology = builder.build()
        .addSource("Scheduler", new ByteArrayDeserializer(), new ByteArrayDeserializer(), topicsConfig.getString("scheduler"))
        .addProcessor(ScheduleProcessor.PROCESSOR_NAME, () -> new ScheduleProcessor(schedulerConfig.getDuration("punctuate.interval")), "Scheduler")
        .addStateStore(storeBuilder, ScheduleProcessor.PROCESSOR_NAME)
        .addProcessor("Original", () -> new ToOriginalRecordProcessor(), ScheduleProcessor.PROCESSOR_NAME)
        .addSink("Outgoing", new TopicNameExtractor<K,V>() {
        }, "Original");


    logger.debug("streams topology: {}", topology.describe());
  
    final KafkaStreams streams = new KafkaStreams(topology, streamsProps);


    streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
      System.exit(70);
    });
    // Shutdown hook to clean up resources
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Executing cleanup as part of shutdown hook");
      consumerRunners.forEach(consumer -> consumer.shutdown());
      try {
        if (!consumerExecutorService.awaitTermination(consumerShutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
          consumerExecutorService.shutdownNow();
        }
      } catch (InterruptedException ex) {
        logger.error("InterruptedException while waiting for ExecutorService to terminate.", ex);
      }
      try {
        producer.close(producerShutdownTimeout);
      } catch (InterruptException ex) {
        logger.error("InterruptException while waiting for producer to close.", ex);
      }
      try {
        streams.close(streamsShutdownTimeout);
      } catch (InterruptException ex) {
        logger.error("InterruptException while waiting for streams to close.", ex);
      }
    }));


    
    // Run each consumer runner
    consumerRunners.stream()
        .forEach(consumer -> consumerEcs.submit(consumer));
    
    // Under ideal operating conditions, consumer threads should never return.
    // If the thread was interrupted, then it will shut down cleanly, returing null
    // In truly exceptional circumstances, the thread may throw an exception
    // In either case we should interrupt the remaining threads and shutdown the app.
    try {
      consumerEcs.take().get();
    } catch (Exception ex) {
      logger.fatal("Caught unexpected and unrecoverable exception", ex);
    }
    logger.info("One or more consumer threads have halted, cleaning up and shutting down.");
    System.exit(70);
  }

}