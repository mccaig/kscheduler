package com.rhysmccaig.kscheduler.streams;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KSchedulerProductionExceptionHandler implements ProductionExceptionHandler {
  static final Logger logger = LogManager.getLogger(KSchedulerProductionExceptionHandler.class); 

  @Override
  public void configure(Map<String, ?> configs) {
      // Nothing to Configure
  }

  @Override
  public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
    // If unknown topic, partition will be null, set it to -1 instead
    var partition = Objects.requireNonNullElse(record.partition(), -1); 
    var topicPartition = new TopicPartition(record.topic(), partition);
    if (exception instanceof UnknownTopicOrPartitionException) {
      logger.warn("Unknown TopicPartition: {}, dropping ({} bytes) message with key: {}, ", 
          topicPartition, 
          record.value().length, 
          record.key(), 
          exception);
      return ProductionExceptionHandlerResponse.CONTINUE;
    } else {
      logger.fatal("ProductionException while producing to TopicPartition: {}, ({} bytes) message with key: {}.", 
          topicPartition, 
          record.value().length, 
          record.key(), 
          exception);
      return ProductionExceptionHandlerResponse.FAIL;
    }
  }
  


}