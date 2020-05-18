package com.rhysmccaig.kschedule;

import com.typesafe.config.ConfigFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.strategy.NotAfterStrategy;
import com.rhysmccaig.kschedule.strategy.NotBeforeStrategy;
import com.rhysmccaig.kschedule.strategy.SchedulerStrategy;
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

    // Set up a consumer for each input/delayed topic 
    // One thread per input topic
    final SchedulerStrategy strategy;
    switch (conf.getString("scheduler.strategy")) {
      case "not_before":
        strategy = new NotBeforeStrategy(delayedTopics, dlqTopic);
        break;
      case "not_after":
        strategy = new NotAfterStrategy(delayedTopics, dlqTopic);
        break;
      case "exact":
        throw new Error("exact strategy not implemented");
      default:
        throw new Error("Unsupported scheduler.strategy");
    }
    Executor consumerExecutor = Executors.newFixedThreadPool(delayedTopics.size());
    delayedTopics.stream().forEach(dt -> consumerExecutor.execute(new DelayedConsumerRunner(consumerConfig, dt, strategy, producer)));
    

    // Need a shutdown hook to clean up the producer and running consumers
    //producer.close();

  }

}