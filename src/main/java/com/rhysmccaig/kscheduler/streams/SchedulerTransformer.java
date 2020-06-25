package com.rhysmccaig.kscheduler.streams;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serialization.ScheduledIdSerde;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordSerde;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.metrics.LongCounter;
import io.opentelemetry.metrics.LongCounter.BoundLongCounter;
import io.opentelemetry.metrics.Meter;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;





public class SchedulerTransformer implements 
    Transformer<ScheduledRecordMetadata, ScheduledRecord, KeyValue<ScheduledRecordMetadata, ScheduledRecord>> {
  static final Logger logger = LogManager.getLogger(SchedulerTransformer.class); 

  // How often we scan the records database for records that are ready to be forwarded.
  public static final Duration DEFAULT_PUNCTUATE_INTERVAL = Duration.ofSeconds(1);
  public static final Duration DEFAULT_MAXIMUM_DELAY = Duration.ofDays(7);
  public static final String DEFAULT_SCHEDULED_RECORDS_STATE_STORE_NAME = "kscheduler-scheduled-records";
  public static final String DEFAULT_SCHEDULED_IDS_STATE_STORE_NAME = "kscheduler-scheduled-ids";
  public static final String PROCESSOR_NAME = "kscheduler-processor";

  private final Meter meter = OpenTelemetry.getMeter("io.opentelemetry.example.metrics", "0.5");

  private static final Serde<UUID> UUID_SERDE = Serdes.UUID();
  private static final Serde<ScheduledId> SCHEDULED_ID_SERDE = new ScheduledIdSerde();
  private static final Serde<ScheduledRecord> SCHEDULED_RECORD_SERDE = new ScheduledRecordSerde();

  private ProcessorContext context;
  private String scheduledRecordStoreName;
  private String scheduledIdStoreName;
  private KeyValueStore<ScheduledId, ScheduledRecord> scheduledRecordStore;
  private KeyValueStore<UUID, ScheduledId> scheduledIdStore;

  private Duration punctuateSchedule;  
  private Duration maximumDelay; 
  private LongCounter recordCounter;
  private BoundLongCounter transformValidCounter;
  private BoundLongCounter transformInvalidCounter;
  private BoundLongCounter scheduledRecordCounter;
  private BoundLongCounter forwardedRecordCounter;

  /**
   * The core scheduling logic of the application.
   * @param scheduledRecordStoreName name of state store for scheduled records
   * @param scheduledIdStoreName name of state store for scheduled records id lookup
   * @param punctuateSchedule how often the scheduler checks for messages to be ready to send
   * @param maximumDelay the maximum amount of time a record may be scheduled in the future
   */
  public SchedulerTransformer(
      String scheduledRecordStoreName, String scheduledIdStoreName, Duration punctuateSchedule, Duration maximumDelay) {
    this.scheduledRecordStoreName = 
        Objects.requireNonNullElse(scheduledRecordStoreName, DEFAULT_SCHEDULED_RECORDS_STATE_STORE_NAME);
    this.scheduledIdStoreName = 
        Objects.requireNonNullElse(scheduledIdStoreName, DEFAULT_SCHEDULED_IDS_STATE_STORE_NAME);
    this.punctuateSchedule = Objects.requireNonNullElse(punctuateSchedule, DEFAULT_PUNCTUATE_INTERVAL);
    this.maximumDelay = Objects.requireNonNullElse(maximumDelay, DEFAULT_MAXIMUM_DELAY);
    recordCounter = meter.longCounterBuilder("processed_records")
        .setDescription("Processed Records")
        .setUnit("Record")
        .build();  
    var transformCounterKey = "transform()";
    transformValidCounter = recordCounter.bind(transformCounterKey, "Valid");
    transformInvalidCounter = recordCounter.bind(transformCounterKey, "Invalid");
    scheduledRecordCounter = recordCounter.bind(transformCounterKey, "Scheduled");
    forwardedRecordCounter = recordCounter.bind("punctuate()", "Forward");
  }

  public SchedulerTransformer() {
    this(null, null, null, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext ctx) {
    context = ctx;
    scheduledRecordStore = 
        (KeyValueStore<ScheduledId, ScheduledRecord>) context.getStateStore(scheduledRecordStoreName);
    scheduledIdStore = (KeyValueStore<UUID, ScheduledId>) context.getStateStore(scheduledIdStoreName);
    // schedule a punctuate() based on wall-clock time
    this.context.schedule(punctuateSchedule, PunctuationType.WALL_CLOCK_TIME, new SchedulerPunctuator());
  }

  /**
   * Add the record into the state store for processing.
   */
  public KeyValue<ScheduledRecordMetadata, ScheduledRecord> 
      transform(ScheduledRecordMetadata metadata, ScheduledRecord record) {
    var id = metadata.id();
    // If a record doesnt have a uuid, its dead to us, so make sure it is set
    if (id == null) {
      transformInvalidCounter.add(1);
    } else {
      transformValidCounter.add(1);
      // Before we do anything, check if we already have a record for this id and remove it from the stores
      var staleRecord = scheduledIdStore.delete(id);
      if (staleRecord != null) {
        scheduledRecordStore.delete(staleRecord);
      }
      // If a records expiry time is after the scheduled time, then add it into our state stores for later processing
      // Ensure that the scheduled time isnt too far in the future
      if (metadata.expires().isAfter(metadata.scheduled())) {
        var recordTimestamp = Instant.ofEpochMilli(context.timestamp());
        if (metadata.scheduled().isBefore(recordTimestamp.plus(maximumDelay))) {
          var sid = new ScheduledId(metadata.scheduled(), metadata.id());
          scheduledRecordStore.put(sid, record);
          scheduledIdStore.put(id, sid);
          scheduledRecordCounter.add(1);
        } else {
          logger.info("Dropped record scheduled after the maximum delay. {}", metadata);
        }
      }
    }
    // We never forward records at this time, only during punctuatation.
    return null;
  }

  /**
   * Release the resources related with this instance.
   */
  public void close() {
    context = null;
    scheduledRecordStore = null;
    scheduledIdStore = null;
    punctuateSchedule = null;
    recordCounter = null;
    transformValidCounter = transformInvalidCounter  = scheduledRecordCounter = forwardedRecordCounter = null;
  }

  public static StoreBuilder<KeyValueStore<ScheduledId, ScheduledRecord>> getScheduledRecordStoreBuilder() {
    return getScheduledRecordStoreBuilder(DEFAULT_SCHEDULED_RECORDS_STATE_STORE_NAME);
  }

  /**
   * Returns a key value storebuilder for scheduled records.
   * @param storeName the name of the store
   * @return
   */
  public static StoreBuilder<KeyValueStore<ScheduledId, ScheduledRecord>> 
      getScheduledRecordStoreBuilder(String storeName) {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        SCHEDULED_ID_SERDE,
        SCHEDULED_RECORD_SERDE)
      .withLoggingEnabled(Collections.emptyMap());
  }

  public static StoreBuilder<KeyValueStore<UUID, ScheduledId>> getScheduledIdStoreBuilder() {
    return getScheduledIdStoreBuilder(DEFAULT_SCHEDULED_IDS_STATE_STORE_NAME);
  }

  /**
   * Returns a key value storebuilder for scheduled record id lookup.
   * @param storeName the name of the store
   * @return
   */
  public static StoreBuilder<KeyValueStore<UUID, ScheduledId>> getScheduledIdStoreBuilder(String storeName) {
    return Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),
        UUID_SERDE,
        SCHEDULED_ID_SERDE)
      .withLoggingEnabled(Collections.emptyMap());
  }


  private class SchedulerPunctuator implements Punctuator {

    @Override
    public void punctuate(long timestamp) {
      var beforeInstant = Instant.ofEpochMilli(timestamp).plusNanos(1);
      var from = new ScheduledId(Instant.MIN, null);
      var to = new ScheduledId(beforeInstant, null);
      // Cant yet define our insertion order, but by default RocksDB orders items lexicographically
      // ScheduledIdSerializer takes this into account
      KeyValueIterator<ScheduledId, ScheduledRecord> iter = scheduledRecordStore.range(from, to);
      var count = 0;
      
      while (iter.hasNext()) {
        KeyValue<ScheduledId, ScheduledRecord> entry = iter.next();
        var value = entry.value;
        var key = value.metadata();
        context.forward(key, value);
        scheduledRecordStore.delete(entry.key);
        count++;
      }
      forwardedRecordCounter.add(count);
      iter.close();
    }

  }


}