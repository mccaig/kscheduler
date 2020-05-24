package com.rhysmccaig.kscheduler;

import com.typesafe.config.ConfigFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;
import com.rhysmccaig.kscheduler.router.Router;
import com.rhysmccaig.kscheduler.router.RoutingStrategy;
import com.rhysmccaig.kscheduler.router.Strategy;
import com.rhysmccaig.kscheduler.util.ConfigUtils;
import com.typesafe.config.Config;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;


public class KScheduler {
  static final Logger logger = LogManager.getLogger(KScheduler.class); 

  public static void main(String[] args) {
    final Config config = ConfigFactory.load();
    final Config scheduleConfig = config.getConfig("scheduler");
    final Integer consumerThreads = scheduleConfig.getInt("consumer.threads");
    final Duration consumerShutdownTimeout = scheduleConfig.getDuration("consumer.shutdown.timeout");
    final Duration producerShutdownTimeout = scheduleConfig.getDuration("producer.shutdown.timeout");

    Properties producerProps = ConfigUtils.toProperties(
        config.getConfig("kafka").withFallback(config.getConfig("kafka.producer")));
    Properties consumerProps = ConfigUtils.toProperties(
        config.getConfig("kafka").withFallback(config.getConfig("kafka.consumer")));

    var delayedTopicsConfig = config.getConfig("topics.delayed");
    var delayedTopicsNames = config.getObject("topics.delayed").keySet();
    List<DelayedTopicConfig> delayedTopics = delayedTopicsNames.stream().map(name -> {
      var topicConfig = delayedTopicsConfig.getConfig(name);
      var delay = topicConfig.getDuration("delay");
      var topic = topicConfig.hasPath("topic") ? topicConfig.getString("topic") : name;
      return new DelayedTopicConfig(name, topic, delay);
    }).collect(Collectors.toList());
    
    final var inputTopic = new DelayedTopicConfig("input", config.getString("topics.input"), Duration.ofSeconds(Long.MIN_VALUE));
    delayedTopics.add(inputTopic);
    
    final var dlqTopic = config.getIsNull("topics.dlq") ? null : config.getString("topics.dlq");
    
    // Set up the producer
    final var producer = new KafkaProducer<byte[],byte[]>(producerProps);

    // Set up a topic router
    final RoutingStrategy defaultRouterStrategy = Strategy.valueOf(config.getString("scheduler.router.strategy"));
    final var topicRouter = new Router(delayedTopics, dlqTopic, producer, defaultRouterStrategy);
    // Set up a consumers
    // One consumer thread per input topic for now
    final var topics = delayedTopics.stream()
        .map(dt -> dt.getTopic())
        .collect(Collectors.toList());
    // Construct consumer runners - one for each desired thread
    final var consumerRunners = new ArrayList<DelayedConsumerRunner>(consumerThreads);
    for (var i = 0; i < consumerThreads; i++) {
      consumerRunners.add(new DelayedConsumerRunner(consumerProps, topics, topicRouter));
    }
    final var consumerExecutorService = Executors.newFixedThreadPool(consumerThreads);
    final CompletionService<Void> consumerEcs = new ExecutorCompletionService<>(consumerExecutorService);

    // Streams
    //final Topology builder = new Topology();
    //builder.addSource("Scheduled", "scheduled")
    //    .addProcessor(name, supplier, parentNames)

    



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