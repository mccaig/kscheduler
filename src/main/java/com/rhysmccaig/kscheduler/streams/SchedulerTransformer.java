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
  public static final Duration DEFAULT_PUNCTUATE_INTERVAL = Duration.ofSeconds(1);
  public static final String DEFAULT_STATE_STORE_NAME = "kscheduler-scheduled";
  public static final String PROCESSOR_NAME = "kscheduler-processor";

  private String stateStoreName;
  private ProcessorContext context;
  private KeyValueStore<ScheduledId, ScheduledRecord> kvStore;
  private Duration punctuateSchedule;
  private HashMap<UUID, ScheduledId> idLookupMap;

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
  public void init(ProcessorContext ctx) {
    context = ctx;
    idLookupMap = new HashMap<>(LOOKUP_TABLE_INITIAL_CAPACITY, LOOKUP_TABLE_LOAD_FACTOR);
    kvStore = (KeyValueStore<ScheduledId, ScheduledRecord>) context.getStateStore(stateStoreName);
    // schedule a punctuate() based on wall-clock time
    this.context.schedule(punctuateSchedule, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
      var notBeforeInstant = Instant.ofEpochMilli(timestamp);
      var beforeInstant = Instant.ofEpochMilli(timestamp).plus(punctuateSchedule);
      var from = new ScheduledId(notBeforeInstant, null);
      var to = new ScheduledId(beforeInstant, null);
      // Cant yet define our insertion order, but by default RocksDB orders items lexicographically
      // ScheduledIdSerializer takes this into account
      KeyValueIterator<ScheduledId, ScheduledRecord> iter = this.kvStore.range(from, to);
      while (iter.hasNext()) {
          KeyValue<ScheduledId, ScheduledRecord> entry = iter.next();
          var value = entry.value;
          var key = value.metadata();
          context.forward(key, value);
          kvStore.delete(entry.key);
      }
      iter.close();
    });
  }

  /**
   * Add the record into the state store for processing
   */
  public KeyValue<ScheduledRecordMetadata, ScheduledRecord> transform(ScheduledRecordMetadata metadata, ScheduledRecord record) {
    var id = metadata.id();
    // If a record doesnt have a uuid, its dead to us, so make sure it is set
    if (id != null) {
      // Before we do anything, check if we already have a record for this uuid and remove it from the kvStore
      var staleRecord = idLookupMap.get(id);
      if (staleRecord != null) {
        kvStore.delete(staleRecord);
        idLookupMap.remove(id);
      }
      // If a records expiry time is after the scheduled time, then add it into our state store for later processing
      if (metadata.expires().isAfter(metadata.scheduled())) {
        var sid = new ScheduledId(metadata.scheduled(), metadata.id());
        idLookupMap.put(id, sid);
        kvStore.put(sid, record);
      }
    }
    // We never forward records at this time, only during punctuatation.
    return null;
  }

  public void close() {
    stateStoreName = null;
    punctuateSchedule = null;
    idLookupMap = null;
  }

}