// package com.rhysmccaig.kscheduler.router;

// import java.time.Duration;
// import java.time.Instant;
// import java.util.Collection;
// import java.util.Map;
// import java.util.SortedSet;
// import java.util.TreeSet;
// import java.util.concurrent.CompletableFuture;
// import java.util.concurrent.Future;
// import java.util.stream.Collectors;

// import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;
// import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
// import com.rhysmccaig.kscheduler.model.TopicPartitionOffset;
// import com.rhysmccaig.kscheduler.util.HeaderUtils;

// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.ProducerRecord;
// import org.apache.kafka.clients.producer.RecordMetadata;
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;

// /**
//  * Routes Scheduled Records to the next hop
//  * Thread Safe.
//  */
// public class Router {
//   static final Logger logger = LogManager.getLogger(Router.class); 

//   public static final RoutingStrategy DEFAULT_ROUTING_STRATEGY = new NotBeforeStrategy();

//   protected final SortedSet<DelayedTopicConfig> delayedTopicsSet;
//   protected final Map<String, Duration> topicDelays;
//   protected final String deadLetterTopic;
//   protected final RoutingStrategy routingStrategy;
//   protected final KafkaProducer<byte[], byte[]> producer;

//   public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, KafkaProducer<byte[], byte[]> producer, RoutingStrategy defaultRoutingStrategy) {
//     this.delayedTopicsSet = delayedTopics.stream()
//         .collect(Collectors.toCollection(() -> 
//             new TreeSet<DelayedTopicConfig>((a, b) -> a.getDelay().compareTo(b.getDelay()))));
//     this.topicDelays = delayedTopics.stream()
//         .collect(Collectors.toMap(DelayedTopicConfig::getName, DelayedTopicConfig::getDelay));
//     this.deadLetterTopic = deadLetterTopic;
//     this.routingStrategy = (defaultRoutingStrategy == null) ? new NotBeforeStrategy() : defaultRoutingStrategy;
//     this.producer = producer;
//   }

//   public Router(Collection<DelayedTopicConfig> delayedTopics, String deadLetterTopic, KafkaProducer<byte[], byte[]>  producer) {
//     this(delayedTopics, deadLetterTopic, producer, DEFAULT_ROUTING_STRATEGY);
//   }

//   public Future<RecordMetadata> forward(final ConsumerRecord<byte[], byte[]> record, ScheduledRecordMetadata metadata) {
//     return forward(record, metadata, Instant.now());
//   }

//   public Future<RecordMetadata> forward(final ConsumerRecord<byte[], byte[]> record, ScheduledRecordMetadata metadata, Instant now) {
//     final Future<RecordMetadata> produceResult;
//     final var source = TopicPartitionOffset.fromConsumerRecord(record);
//     if (metadata == null) {
//       var reason = new StringBuilder()
//           .append("No KScheduler metadata present")
//           .append(source)
//           .toString();
//       produceResult = drop(record, reason, now);
//     } else if (metadata.expires() != null && metadata.expires().isBefore(now)) {
//       var reason = new StringBuilder()
//           .append("expired record: ")
//           .append(source)
//           .toString();
//       produceResult = drop(record, reason, now);
//     } else {
//       // All clear, continue to route the event
//       var nextTopic = routingStrategy.nextTopic(delayedTopicsSet, metadata, now);
//       produceResult = send(record, metadata, nextTopic, now);
//     }
//     return produceResult;
//   }

//   public Future<RecordMetadata> drop(final ConsumerRecord<byte[], byte[]> record, String reason) {
//     return drop(record, reason, Instant.now());
//   }

//   public Future<RecordMetadata> drop(final ConsumerRecord<byte[], byte[]> record, String reason, Instant now) {
//     logger.info("dropping record: {}", reason);
//     if (deadLetterTopic != null) {
//       HeaderUtils.addError(record.headers(), reason);
//       return send(record, null, deadLetterTopic, now);
//     } else {
//       return CompletableFuture.completedFuture(null);
//     }
//   }

//   public Future<RecordMetadata> send(final ConsumerRecord<byte[], byte[]> record, ScheduledRecordMetadata metadata, String destination, Instant now) {
//     if (metadata != null) {
//       metadata.setProduced(now);
//     }
//     return producer.send(new ProducerRecord<byte[], byte[]>(destination, null, null, record.key(), record.value(), HeaderUtils.setMetadata(record.headers(), metadata)));
//   }

//   public Instant processAt(TopicPartitionOffset source, ScheduledRecordMetadata metadata) {
//     final var delay = topicDelays.get(source.topic());
//     final Instant delayUntil;
//     if ((delay == null) || metadata == null || metadata.produced() == null) {
//       delayUntil = Instant.MIN;
//     } else {
//       delayUntil = metadata.produced().plus(delay);
//     }
//     return delayUntil;
//   }

// }