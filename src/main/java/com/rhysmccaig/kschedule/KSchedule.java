package com.rhysmccaig.kschedule;

import com.typesafe.config.ConfigFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.router.Router;
import com.rhysmccaig.kschedule.router.RoutingStrategy;
import com.rhysmccaig.kschedule.router.Strategy;
import com.rhysmccaig.kschedule.util.ConfigUtils;
import com.typesafe.config.Config;


public class KSchedule {
  
  static final Logger logger = LogManager.getLogger(KSchedule.class); 

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
    var inputTopic = new DelayedTopicConfig("input", conf.getString("topics.input"), Duration.ofSeconds(Long.MIN_VALUE));
    delayedTopics.add(inputTopic);
    var dlqTopic = conf.getIsNull("topics.dlq") ? null : conf.getString("topics.dlq");
    // Set up the producer
    var producer = new KafkaProducer<byte[],byte[]>(producerConfig);

    // Set up a topic router
    final RoutingStrategy defaultRouterStrategy = Strategy.valueOf(conf.getString("scheduler.router.strategy"));
    final Router topicRouter = new Router(delayedTopics, dlqTopic, defaultRouterStrategy, producer);
    // Set up a consumer for each input/delayed topic 
    // One consumer thread per input topic for now
    ExecutorService consumerExecutor = Executors.newFixedThreadPool(delayedTopics.size());
    var consumers = delayedTopics.stream()
        .map(delayedTopic -> new DelayedConsumerRunner(consumerConfig, delayedTopic, topicRouter))
        .collect(Collectors.toList());
    
    var threads = consumerExecutor.invokeAll(consumers);
    

    // Need a shutdown hook to clean up the producer and running consumers
    //producer.close();

  }

}