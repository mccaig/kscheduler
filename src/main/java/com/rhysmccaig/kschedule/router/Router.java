package com.rhysmccaig.kschedule.router;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;
import com.rhysmccaig.kschedule.router.RouterStrategy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class Router {

  protected final SortedSet<DelayedTopicConfig> delayedTopicsSet;
  protected final String deadLetterTopic;
  protected final RouterStrategy defaultRouterStrategy;
  protected final KafkaProducer<byte[], byte[]> producer;

  public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, RouterStrategy defaultRouterStrategy, KafkaProducer<byte[], byte[]> producer) {
    this.delayedTopicsSet = delayedTopics.stream()
        .collect(Collectors.toCollection(() -> 
            new TreeSet<DelayedTopicConfig>((a, b) -> a.getDelay().compareTo(b.getDelay()))));
    this.deadLetterTopic = deadLetterTopic;
    this.defaultRouterStrategy = defaultRouterStrategy;
    this.producer = producer;
  }

  public void route(byte[] key, byte[] value, Headers headers) {

  }

  public Future<RecordMetadata> routeDeadLetter(byte[] key, byte[] value, List<Header> headers) {
    return producer.send(new ProducerRecord<byte[],byte[]>(deadLetterTopic, null, null, key, value, headers));
  }

  

}