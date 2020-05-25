package com.rhysmccaig.kscheduler.model;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class TopicPartitionOffset {
    
  private final String topic;
  private final int partition;
  private final long offset;

  public TopicPartitionOffset(String topic, int partition, long offset) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  public TopicPartition getTopicPartition() {
    return new TopicPartition(topic, partition);
  }

  public static TopicPartitionOffset fromConsumerRecord(ConsumerRecord<?, ?> consumerRecord) {
    return new TopicPartitionOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(TopicPartitionOffset.class.getSimpleName())
        .append("[")
        .append("topic=")
        .append(topic)
        .append(",partition=")
        .append(partition)
        .append(",offset=")
        .append(offset)
        .append("]")
        .toString(); 
  }
  
}