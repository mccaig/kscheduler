package com.rhysmccaig.kscheduler;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.TopicSettings;
import com.rhysmccaig.kscheduler.serialization.ScheduledIdSerde;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerde;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordSerde;
import com.rhysmccaig.kscheduler.streams.ScheduledDestinationTopicNameExtractor;
import com.rhysmccaig.kscheduler.streams.ScheduledRecordIdPartitioner;
import com.rhysmccaig.kscheduler.streams.ScheduledToDestinationTransformer;
import com.rhysmccaig.kscheduler.streams.SchedulerTransformer;
import com.rhysmccaig.kscheduler.streams.SourceToScheduledTransformer;
import io.quarkus.kafka.client.serialization.JsonbSerde;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.eclipse.microprofile.config.inject.ConfigProperty;



@ApplicationScoped
public class KSchedulerTopologyProducer {

  public static final String SCHEDULED_RECORD_STATE_STORE_NAME = "kscheduler-scheduled-records";
  public static final String SCHEDULED_ID_MAPPING_STATE_STORE_NAME = "kscheduler-scheduled-id-mappings";
  public static final String TOPIC_SETTINGS_STATE_STORE_NAME = "kscheduler-topic-settings";
  private static final Serde<UUID> UUID_SERDE = Serdes.UUID();
  private static final Serde<ScheduledId> SCHEDULED_ID_SERDE = new ScheduledIdSerde();
  private static final Serde<ScheduledRecord> SCHEDULED_RECORD_SERDE = new ScheduledRecordSerde();
  private static final Serde<ScheduledRecordMetadata> SCHEDULED_RECORD_METADATA_SERDE = new ScheduledRecordMetadataSerde();
  private static final JsonbSerde<TopicSettings> TOPIC_SETTINGS_SERDE = new JsonbSerde<>(TopicSettings.class);

  @ConfigProperty(name = "kscheduler.topics.input")
  String inputTopic;
  @ConfigProperty(name = "kscheduler.topics.scheduled")
  String scheduledTopic;
  @ConfigProperty(name = "kscheduler.topics.outgoing")
  String outgoingTopic;
  @ConfigProperty(name = "kscheduler.topics.dlq")
  String dlqTopic;
  @ConfigProperty(name = "kscheduler.topics.topic-settings")
  String denylistTopic;
  @ConfigProperty(name = "kscheduler.punctuate.schedule")
  Duration punctuateSchedule;
  @ConfigProperty(name = "kscheduler.maximum.delay")
  Duration maximumDelay;
  @ConfigProperty(name = "kscheduler.defaults.scheduling-enabled")
  boolean schedulingEnabled;
  @ConfigProperty(name = "kscheduler.defaults.ignore-topic-errors")
  boolean ignoreTopicErrors;

  @Produces
  public Topology getTopology() {
    var defaultTopicSettings = new TopicSettings(schedulingEnabled, ignoreTopicErrors);
    var scheduledRecordStoreBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(SCHEDULED_RECORD_STATE_STORE_NAME),
        UUID_SERDE,
        SCHEDULED_RECORD_SERDE)
      .withLoggingEnabled(Collections.emptyMap());
    var scheduledIdMappingStoreBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(SCHEDULED_ID_MAPPING_STATE_STORE_NAME),
        SCHEDULED_ID_SERDE,
        UUID_SERDE)
      .withLoggingEnabled(Collections.emptyMap());
    // topology
    var builder = new StreamsBuilder();
    builder.addStateStore(scheduledRecordStoreBuilder)
        .addStateStore(scheduledIdMappingStoreBuilder);
    // create denylist ktable and materialize into a state store
    builder.globalTable(
        denylistTopic, 
        Consumed.with(Serdes.String(), TOPIC_SETTINGS_SERDE), 
        Materialized.as(TOPIC_SETTINGS_STATE_STORE_NAME));
    // create main app stream
    builder.stream(inputTopic, Consumed.with(Serdes.Bytes(), Serdes.Bytes()))
        .transform(
            () -> new SourceToScheduledTransformer(TOPIC_SETTINGS_STATE_STORE_NAME, defaultTopicSettings), 
            Named.as("SOURCE_TO_SCHEDULED"))
        .through(scheduledTopic, Produced.with(SCHEDULED_RECORD_METADATA_SERDE, SCHEDULED_RECORD_SERDE, new ScheduledRecordIdPartitioner()))
        .transform(
            () -> new SchedulerTransformer(
                SCHEDULED_RECORD_STATE_STORE_NAME, 
                SCHEDULED_ID_MAPPING_STATE_STORE_NAME,
                punctuateSchedule,
                maximumDelay),
            Named.as("SCHEDULER"), SCHEDULED_RECORD_STATE_STORE_NAME, SCHEDULED_ID_MAPPING_STATE_STORE_NAME)
        .through(outgoingTopic, Produced.with(SCHEDULED_RECORD_METADATA_SERDE, SCHEDULED_RECORD_SERDE, new ScheduledRecordIdPartitioner()))
        .transform(
            () -> new ScheduledToDestinationTransformer(TOPIC_SETTINGS_STATE_STORE_NAME, defaultTopicSettings), 
            Named.as("SCHEDULED_TO_SOURCE"))
        .to(new ScheduledDestinationTopicNameExtractor(dlqTopic), Produced.with(Serdes.Bytes(), Serdes.Bytes()));
    return builder.build();
  }

}