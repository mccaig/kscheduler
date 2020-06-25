package com.rhysmccaig.kscheduler;

import com.typesafe.config.ConfigFactory;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.streams.SchedulerTransformer;
import com.rhysmccaig.kscheduler.streams.SourceToScheduledTransformer;
import com.rhysmccaig.kscheduler.streams.KSchedulerProductionExceptionHandler;
import com.rhysmccaig.kscheduler.streams.ScheduledDestinationTopicNameExtractor;
import com.rhysmccaig.kscheduler.streams.ScheduledRecordIdPartitioner;
import com.rhysmccaig.kscheduler.streams.ScheduledToSourceTransformer;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerde;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordSerde;
import com.rhysmccaig.kscheduler.util.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;


public class KScheduler {
  static final Logger logger = LogManager.getLogger(KScheduler.class); 

  private static Serde<ScheduledRecordMetadata> METADATA_SERDE = new ScheduledRecordMetadataSerde();
  private static Serde<ScheduledRecord> RECORD_SERDE = new ScheduledRecordSerde();

  public static void main(String[] args) {
    final Config config = ConfigFactory.load();
    final Config schedulerConfig = config.getConfig("scheduler");
    final Config topicsConfig = config.getConfig("topics");
    final Config streamsConfig = config.getConfig("kafka.streams");

    final Duration streamsShutdownTimeout = schedulerConfig.getDuration("streams.shutdown.timeout");

    Properties streamsProps = ConfigUtils.toProperties(streamsConfig);
    // Force these settings
    streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    streamsProps.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, KSchedulerProductionExceptionHandler.class);

    final Topology topology = getTopology(
        topicsConfig.getString("input"), 
        topicsConfig.getString("scheduled"),
        topicsConfig.getString("outgoing"), 
        SchedulerTransformer.getScheduledRecordStoreBuilder(topicsConfig.getString("scheduled-records")),
        SchedulerTransformer.getScheduledIdStoreBuilder(topicsConfig.getString("scheduled-ids")),
        schedulerConfig.getDuration("punctuate.interval"),
        schedulerConfig.getDuration("maximum.delay"));

    logger.debug("streams topology: {}", topology.describe());

    try (KafkaStreams streams = new KafkaStreams(topology, streamsProps)) {
      streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
        logger.fatal("Uncaught Exception.", throwable);
        System.exit(70);
      });
      // Shutdown hook to clean up resources
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Executing cleanup as part of shutdown hook");
        try {
          streams.close(streamsShutdownTimeout);
        } catch (InterruptException ex) {
          logger.error("InterruptException while waiting for streams to close.", ex);
        }
      }));
      streams.start();
    }

  }



  
  public static Topology getTopology(
      String inputTopic,
      String scheduledTopic,
      String outgoingTopic,
      StoreBuilder<KeyValueStore<ScheduledId, ScheduledRecord>> scheduledRecordStoreBuilder,
      StoreBuilder<KeyValueStore<UUID, ScheduledId>> scheduledIdStoreBuilder,
      Duration punctuateInterval,
      Duration maximumDelay) {
    var builder = new StreamsBuilder();
    builder.addStateStore(scheduledRecordStoreBuilder)
        .addStateStore(scheduledIdStoreBuilder)
        .stream(inputTopic, Consumed.with(Serdes.Bytes(), Serdes.Bytes()))
        .transform(SourceToScheduledTransformer::new, Named.as("SOURCE_TO_SCHEDULED"))
        .through(scheduledTopic, Produced.with(METADATA_SERDE, RECORD_SERDE, new ScheduledRecordIdPartitioner()))
        .transform(
            () -> new SchedulerTransformer(scheduledRecordStoreBuilder.name(), scheduledIdStoreBuilder.name(), punctuateInterval, maximumDelay), 
            Named.as("SCHEDULER"),
            scheduledRecordStoreBuilder.name(),
            scheduledIdStoreBuilder.name())
        .through(outgoingTopic, Produced.with(METADATA_SERDE, RECORD_SERDE, new ScheduledRecordIdPartitioner()))
        .transform(ScheduledToSourceTransformer::new, Named.as("SCHEDULED_TO_SOURCE"))
        .to(new ScheduledDestinationTopicNameExtractor(), Produced.with(Serdes.Bytes(), Serdes.Bytes()));
    return builder.build();
  }

}