package com.rhysmccaig.kscheduler.streams;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KSchedulerProductionExceptionHandler implements ProductionExceptionHandler {
  static final Logger logger = LogManager.getLogger(KSchedulerProductionExceptionHandler.class); 

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
    var topicPartition = new TopicPartition(record.topic(), record.partition());
    if (exception instanceof UnknownTopicOrPartitionException) {
      logger.warn("Unknown TopicPartition: {}, dropping ({} bytes) message with key: {}, ", topicPartition, record.value().length, record.key());
      return ProductionExceptionHandlerResponse.CONTINUE;
    } else {
      logger.fatal("ProductionException while producing to TopicPartition: {}, ({} bytes) message with key: {}. Exception: {}", topicPartition, record.value().length, record.key(), exception);
      return ProductionExceptionHandlerResponse.FAIL;
    }
  }
  


}