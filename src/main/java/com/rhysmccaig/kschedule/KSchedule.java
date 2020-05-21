package com.rhysmccaig.kschedule;

import com.typesafe.config.ConfigFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.router.Router;
import com.rhysmccaig.kschedule.router.RoutingStrategy;
import com.rhysmccaig.kschedule.router.Strategy;
import com.rhysmccaig.kschedule.util.ConfigUtils;
import com.typesafe.config.Config;


public class KSchedule {

  static final Logger logger = LogManager.getLogger(KSchedule.class); 

  // TODO: Move the shutdown timeouts into config
  public static final Integer CONSUMER_SHUTDOWN_TIMEOUT_MS = 15000;
  public static final Duration PRODUCER_SHUTDOWN_TIMEOUT_MS = Duration.ofMillis(15000);


  public static void main(String[] args) {
    Config conf = ConfigFactory.load();

    Properties producerConfig = ConfigUtils.toProperties(
        conf.getConfig("kafka").withFallback(conf.getConfig("kafka.producer")));
    Properties consumerConfig = ConfigUtils.toProperties(
        conf.getConfig("kafka").withFallback(conf.getConfig("kafka.consumer")));

    var delayedTopicsConfig = conf.getConfig("topics.delayed");
    var delayedTopicsNames = conf.getObject("topics.delayed").keySet();
    List<DelayedTopicConfig> delayedTopics = delayedTopicsNames.stream().map(name -> {
      var topicConfig = delayedTopicsConfig.getConfig(name);
      var delay = topicConfig.getDuration("delay");
      var topic = topicConfig.hasPath("topic") ? topicConfig.getString("topic") : name;
      return new DelayedTopicConfig(name, topic, delay);
    }).collect(Collectors.toList());
    final var inputTopic = new DelayedTopicConfig("input", conf.getString("topics.input"), Duration.ofSeconds(Long.MIN_VALUE));
    delayedTopics.add(inputTopic);
    final var dlqTopic = conf.getIsNull("topics.dlq") ? null : conf.getString("topics.dlq");
    // Set up the producer
    final var producer = new KafkaProducer<byte[],byte[]>(producerConfig);

    // Set up a topic router
    final RoutingStrategy defaultRouterStrategy = Strategy.valueOf(conf.getString("scheduler.router.strategy"));
    final var topicRouter = new Router(delayedTopics, dlqTopic, defaultRouterStrategy, producer);
    // Set up a consumer for each input/delayed topic 
    // One consumer thread per input topic for now
    final var consumerExecutorService = Executors.newFixedThreadPool(delayedTopics.size());
    final CompletionService<Void> consumerEcs = new ExecutorCompletionService<>(consumerExecutorService);
    final var consumers = delayedTopics.stream()
        .map(delayedTopic -> new DelayedConsumerRunner(consumerConfig, delayedTopic, topicRouter))
        .collect(Collectors.toList());
    final var consumerFutures = consumers.stream()
        .map(consumer -> consumerEcs.submit(consumer))
        .collect(Collectors.toList());
    
    // Under ideal operating conditions, consumer threads should never return.
    // If the thread was interrupted, then it will shut down cleanly, returing null
    // In exceptional circumstances, the thread may throw an exception
    // In either case we should interrupt the remaining threads and shutdown the app.
    try {
      consumerEcs.take().get();
    } catch (CancellationException 
        | ExecutionException 
        | InterruptedException ex) {
      // These are the most likely exceptions
      logger.fatal("Caught expected, but unrecoverable exception, shutting down.", ex);
    } catch (Exception ex) {
      logger.fatal("Caught unexpected and unrecoverable exception, shutting down.", ex);
    } finally {
      logger.info("Attempting to clean up");
      consumers.forEach(consumer -> {
        consumer.shutdown();
      });
    }
    consumerExecutorService.shutdown();
    try {
      if (!consumerExecutorService.awaitTermination(CONSUMER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        consumerExecutorService.shutdownNow();
      }
    } catch (InterruptedException ex) {
      // Well.. we tried
      logger.error("Timeout while waiting for consumer threads to terminate.", ex);
    }
    producer.close(PRODUCER_SHUTDOWN_TIMEOUT_MS);

  }

}