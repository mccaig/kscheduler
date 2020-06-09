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
import com.rhysmccaig.kscheduler.streams.KSchedulerProductionExceptionHandler;
import com.rhysmccaig.kscheduler.streams.ScheduledDestinationTopicNameExtractor;
import com.rhysmccaig.kscheduler.streams.ScheduledToSourceTransformer;
import com.rhysmccaig.kscheduler.serdes.ScheduledIdSerde;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordSerde;
import com.rhysmccaig.kscheduler.util.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
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

    //final Duration producerShutdownTimeout = schedulerConfig.getDuration("producer.shutdown.timeout");
    final Duration streamsShutdownTimeout = schedulerConfig.getDuration("streams.shutdown.timeout");

    //Properties producerProps = ConfigUtils.toProperties(kafkaConfig.withFallback(kafkaConfig.getConfig("producer")));
    Properties streamsProps = ConfigUtils.toProperties(kafkaConfig.withFallback(kafkaConfig.getConfig("streams")));
    streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    streamsProps.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, KSchedulerProductionExceptionHandler.class);

    // Set up the producer
    //final var producer = new KafkaProducer<byte[], byte[]>(producerProps);
      
    final Topology topology = getTopology(topicsConfig.getString("input"), schedulerConfig.getDuration("punctuate.interval"), getStoreBuilder());

    logger.debug("streams topology: {}", topology.describe());

    System.out.println(topology.describe());
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
        //producer.close(producerShutdownTimeout);
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

  public static StoreBuilder<KeyValueStore<ScheduledId, ScheduledRecord>> getStoreBuilder() {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(SchedulerTransformer.DEFAULT_STATE_STORE_NAME),
        new ScheduledIdSerde(),
        new ScheduledRecordSerde())
      .withLoggingEnabled(Collections.emptyMap());
  }

  public static Topology getTopology(
      String inputTopic, 
      Duration punctuateInterval,
      StoreBuilder<KeyValueStore<ScheduledId, ScheduledRecord>> storeBuilder) {
    var builder = new StreamsBuilder();
    builder.addStateStore(storeBuilder)
        .stream(inputTopic, Consumed.with(Serdes.Bytes(), Serdes.Bytes()))
        .transform(() -> new SourceToScheduledTransformer(), Named.as("SOURCE_TO_SCHEDULED"))
        .transform(() -> new SchedulerTransformer(punctuateInterval), Named.as("SCHEDULER"), SchedulerTransformer.DEFAULT_STATE_STORE_NAME)
        .transform(() -> new ScheduledToSourceTransformer(), Named.as("SCHEDULED_TO_SOURCE"))
        .to(new ScheduledDestinationTopicNameExtractor(), Produced.with(Serdes.Bytes(), Serdes.Bytes()));
    return builder.build();
  }

}