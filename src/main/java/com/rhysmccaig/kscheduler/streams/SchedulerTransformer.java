package com.rhysmccaig.kscheduler.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

public class SchedulerTransformer implements Transformer<ScheduledRecordMetadata, ScheduledRecord, KeyValue<ScheduledRecordMetadata, ScheduledRecord>> {
  static final Logger logger = LogManager.getLogger(SchedulerTransformer.class); 

  public static final int LOOKUP_TABLE_INITIAL_CAPACITY = 8192;
  public static final float LOOKUP_TABLE_LOAD_FACTOR = 0.75f;

  // How often we scan the records database for records that are ready to be forwarded.
  public static final Duration DEFAULT_PUNCTUATE_INTERVAL = Duration.ofSeconds(5);
  public static final String DEFAULT_STATE_STORE_NAME = "kscheduler-scheduled";
  public static final String PROCESSOR_NAME = "kscheduler-processor";
  private static final UUID MIN_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final UUID MAX_UUID = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

  private String stateStoreName;
  private ProcessorContext context;
  private KeyValueStore<ScheduledId, ScheduledRecord> kvStore;
  private Duration punctuateSchedule;
  private HashMap<UUID, ScheduledId> idLookupTable;

  public SchedulerTransformer(String stateStoreName, Duration punctuateSchedule) {
    this.stateStoreName = Objects.requireNonNull(stateStoreName);
    this.punctuateSchedule = Objects.requireNonNull(punctuateSchedule);
  }

  public SchedulerTransformer(Duration punctuateSchedule) {
    this(DEFAULT_STATE_STORE_NAME, punctuateSchedule);
  }

  public SchedulerTransformer() {
    this(DEFAULT_STATE_STORE_NAME, DEFAULT_PUNCTUATE_INTERVAL);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    this.idLookupTable = new HashMap<>(LOOKUP_TABLE_INITIAL_CAPACITY, LOOKUP_TABLE_LOAD_FACTOR);
    kvStore = (KeyValueStore<ScheduledId, ScheduledRecord>) context.getStateStore(stateStoreName);
    // schedule a punctuate() based on wall-clock time
    this.context.schedule(punctuateSchedule, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
      var notBeforeInstant = Instant.ofEpochMilli(timestamp);
      var beforeInstant = Instant.ofEpochMilli(timestamp).plus(punctuateSchedule).minusNanos(1);
      // TODO: Consider allowing scheduledId to be null so we dont have to do this
      var from = new ScheduledId(notBeforeInstant, MIN_UUID);
      var to = new ScheduledId(beforeInstant, MAX_UUID);
      // Cant yet define our insertion order, but by default RocksDB orders items lexicographically
      // ScheduledIdSerializer takes this into account
      KeyValueIterator<ScheduledId, ScheduledRecord> iter = this.kvStore.range(from, to);
      while (iter.hasNext()) {
          KeyValue<ScheduledId, ScheduledRecord> entry = iter.next();
          var value = entry.value;
          var key = value.metadata();
          assert (entry.key.scheduled().isAfter(notBeforeInstant.minus(Duration.ofNanos(1))));
          assert (value.metadata().scheduled().isAfter(notBeforeInstant.minus(Duration.ofNanos(1))));
          assert (entry.key.scheduled().isBefore(beforeInstant));
          assert (value.metadata().scheduled().isBefore(beforeInstant));
          context.forward(key, value);
          this.kvStore.delete(entry.key);
      }
      iter.close();
    });
  }

  /**
   * Add the record into the state store for processing
   */
  public KeyValue<ScheduledRecordMetadata, ScheduledRecord> transform(ScheduledRecordMetadata key, ScheduledRecord value) {
    // Before we do anything, check if we alrea
    // TODO: Implement hashmap for keys
    var sid = new ScheduledId(key.scheduled(), key.id());
    this.kvStore.put(sid, value);
    return null;
  }

  public void close() {
    stateStoreName = null;
    punctuateSchedule = null;
  }

}