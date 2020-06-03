package com.rhysmccaig.kscheduler;

import com.typesafe.config.ConfigFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.streams.SchedulerTransformer;
import com.rhysmccaig.kscheduler.streams.SourceToScheduledTransformer;
import com.rhysmccaig.kscheduler.streams.ScheduledDestinationTopicNameExtractor;
import com.rhysmccaig.kscheduler.streams.ScheduledToSourceTransformer;
import com.rhysmccaig.kscheduler.serdes.ScheduledIdSerde;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordSerde;
import com.rhysmccaig.kscheduler.util.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
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

    final Duration producerShutdownTimeout = schedulerConfig.getDuration("producer.shutdown.timeout");
    final Duration streamsShutdownTimeout = schedulerConfig.getDuration("streams.shutdown.timeout");

    Properties producerProps = ConfigUtils.toProperties(kafkaConfig.withFallback(kafkaConfig.getConfig("producer")));
    Properties streamsProps = ConfigUtils.toProperties(kafkaConfig.withFallback(kafkaConfig.getConfig("streams")));

    // Set up the producer
    final var producer = new KafkaProducer<byte[], byte[]>(producerProps);


    StoreBuilder<KeyValueStore<ScheduledId, ScheduledRecord>> storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(SchedulerTransformer.DEFAULT_STATE_STORE_NAME),
        new ScheduledIdSerde(),
        new ScheduledRecordSerde())
      .withLoggingEnabled(Collections.emptyMap());
    
    //var scheduledTopic = topicsConfig.getString("scheduler");
    var builder = new StreamsBuilder();
    // Input topic
    builder.stream(topicsConfig.getString("input"), Consumed.with(Serdes.Bytes(), Serdes.Bytes()))
        .transform(() -> new SourceToScheduledTransformer(), Named.as("SOURCE_TO_SCHEDULED"))
        .transform(() -> new SchedulerTransformer(schedulerConfig.getDuration("punctuate.interval")), Named.as("SCHEDULER"), SchedulerTransformer.DEFAULT_STATE_STORE_NAME)
        .transform(() -> new ScheduledToSourceTransformer(), Named.as("SCHEDULED_TO_SOURCE"))
        .to(new ScheduledDestinationTopicNameExtractor(), Produced.with(Serdes.Bytes(), Serdes.Bytes()));
        //.to(scheduledTopic, Produced.with(new ScheduledRecordMetadataSerde(), new ScheduledRecordSerde()));
      
    final Topology topology = builder.build()
        .addStateStore(storeBuilder, SchedulerTransformer.PROCESSOR_NAME);

    logger.debug("streams topology: {}", topology.describe());
    try {
      Thread.sleep(10000);
    } catch (Exception ex) {}
    final KafkaStreams streams = new KafkaStreams(topology, streamsProps);

    streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
      System.exit(70);
    });
    // Shutdown hook to clean up resources
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Executing cleanup as part of shutdown hook");
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
    
  }

}