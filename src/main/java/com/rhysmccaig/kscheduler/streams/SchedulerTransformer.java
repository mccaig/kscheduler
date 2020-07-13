package com.rhysmccaig.kscheduler.streams;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serialization.ScheduledIdSerde;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordSerde;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;






public class SchedulerTransformer implements 
    Transformer<ScheduledRecordMetadata, ScheduledRecord, KeyValue<ScheduledRecordMetadata, ScheduledRecord>> {
  static final Logger logger = LoggerFactory.getLogger(SchedulerTransformer.class); 

  // How often we scan the records database for records that are ready to be forwarded.
  public static final Duration DEFAULT_PUNCTUATE_INTERVAL = Duration.ofSeconds(1);
  public static final Duration DEFAULT_MAXIMUM_DELAY = Duration.ofDays(7);

  public static final String PROCESSOR_NAME = "kscheduler-processor";

  //private final Meter meter = OpenTelemetry.getMeter("io.opentelemetry.example.metrics", "0.5");

  private ProcessorContext context;
  private String scheduledRecordStoreName;
  private String scheduledIdStoreName;
  private KeyValueStore<UUID, ScheduledRecord> scheduledRecordStore;
  private KeyValueStore<ScheduledId, UUID> scheduledIdMappingStore;

  private Duration punctuateSchedule;  
  private Duration maximumDelay; 
  // private LongCounter recordCounter;
  // private BoundLongCounter transformValidCounter;
  // private BoundLongCounter transformInvalidCounter;
  // private BoundLongCounter scheduledRecordCounter;
  // private BoundLongCounter forwardedRecordCounter;

  /**
   * The core scheduling logic of the application.
   * @param scheduledRecordStoreName name of state store for scheduled records
   * @param scheduledIdMappingStoreName name of state store for scheduled records id lookup
   * @param punctuateSchedule how often the scheduler checks for messages to be ready to send
   * @param maximumDelay the maximum amount of time a record may be scheduled in the future
   */
  public SchedulerTransformer(
      String scheduledRecordStoreName, 
      String scheduledIdMappingStoreName, 
      Duration punctuateSchedule, 
      Duration maximumDelay) {
    this.scheduledRecordStoreName = Objects.requireNonNull(scheduledRecordStoreName);
    this.scheduledIdStoreName = Objects.requireNonNull(scheduledIdMappingStoreName);
    this.punctuateSchedule = Objects.requireNonNullElse(punctuateSchedule, DEFAULT_PUNCTUATE_INTERVAL);
    this.maximumDelay = Objects.requireNonNullElse(maximumDelay, DEFAULT_MAXIMUM_DELAY);
    // recordCounter = meter.longCounterBuilder("processed_records")
    //     .setDescription("Processed Records")
    //     .setUnit("Record")
    //     .build();  
    // var transformCounterKey = "transform()";
    // transformValidCounter = recordCounter.bind(transformCounterKey, "Valid");
    // transformInvalidCounter = recordCounter.bind(transformCounterKey, "Invalid");
    // scheduledRecordCounter = recordCounter.bind(transformCounterKey, "Scheduled");
    // forwardedRecordCounter = recordCounter.bind("punctuate()", "Forward");
  }

  public SchedulerTransformer() {
    this(null, null, null, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext ctx) {
    context = ctx;
    scheduledRecordStore = 
        (KeyValueStore<UUID, ScheduledRecord>) context.getStateStore(scheduledRecordStoreName);
    scheduledIdMappingStore = (KeyValueStore<ScheduledId, UUID>) context.getStateStore(scheduledIdStoreName);
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
      // transformInvalidCounter.add(1);
    } else {
      // transformValidCounter.add(1);
      // Before we do anything, check if we already have a record for this id and remove it from the stores
      var staleRecord = scheduledRecordStore.delete(id);
      if (staleRecord != null) {
        scheduledIdMappingStore.delete(new ScheduledId(staleRecord.metadata().scheduled(), id));
      }
      // If a records expiry time is after the scheduled time, then add it into our state stores for later processing
      if (metadata.expires().isAfter(metadata.scheduled())) {
        var recordTimestamp = Instant.ofEpochMilli(context.timestamp());
        // Ensure that the scheduled time isnt too far in the future past our configured limit
        if (metadata.scheduled().isBefore(recordTimestamp.plus(maximumDelay))) {
          var sid = new ScheduledId(metadata.scheduled(), metadata.id());
          scheduledRecordStore.put(id, record);
          scheduledIdMappingStore.put(sid, id);
          // scheduledRecordCounter.add(1);
        } else {
          // TODO: Add metric
          logger.debug("Dropping record scheduled after the maximum delay. {}", metadata);
        }
      } else {
        // TODO: Add metric
        logger.debug("Dropping record which expires before the scheduled time. {}", metadata);
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
    scheduledIdMappingStore = null;
    punctuateSchedule = null;
    // recordCounter = null;
    // transformValidCounter = transformInvalidCounter  = scheduledRecordCounter = forwardedRecordCounter = null;
  }




  private class SchedulerPunctuator implements Punctuator {

    @Override
    public void punctuate(long timestamp) {
      var beforeInstant = Instant.ofEpochMilli(timestamp).plusNanos(1);
      var from = new ScheduledId(Instant.MIN, null);
      var to = new ScheduledId(beforeInstant, null);
      // Unfortunately there isnt yet a way in kafka streams to define a comparator function for ordering
      // By default RocksDB orders items lexicographically, ScheduledIdSerializer takes this into account
      // and serializes the object sccordingly
      KeyValueIterator<ScheduledId, UUID> iter = scheduledIdMappingStore.range(from, to);
      // var count = 0;
      while (iter.hasNext()) {
        // mapping kv
        KeyValue<ScheduledId, UUID> mappingEntry = iter.next();
        var uuid = scheduledIdMappingStore.delete(mappingEntry.key);
        var value = scheduledRecordStore.delete(uuid);
        var key = value.metadata();
        context.forward(key, value);
        // count++;
      }
      // forwardedRecordCounter.add(count);
      iter.close();
      context.commit();
    }

  }


}