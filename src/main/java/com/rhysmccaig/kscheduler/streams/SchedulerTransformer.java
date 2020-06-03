package com.rhysmccaig.kscheduler.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

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

  // How often we scan the records database for records that are ready to be forwarded.
  public static final Duration DEFAULT_PUNCTUATE_INTERVAL = Duration.ofSeconds(5);
  public static final String DEFAULT_STATE_STORE_NAME = "kscheduler-scheduled";
  public static final String PROCESSOR_NAME = "kscheduler-processor";

  private final String stateStoreName;
  private ProcessorContext context;
  private KeyValueStore<ScheduledId, ScheduledRecord> kvStore;
  private final Duration punctuateSchedule;

  public SchedulerTransformer(String stateStoreName, Duration punctuateSchedule) {
    Objects.requireNonNull(stateStoreName);
    Objects.requireNonNull(punctuateSchedule);
    this.stateStoreName = stateStoreName;
    this.punctuateSchedule = punctuateSchedule;
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
    var sid = new ScheduledId(key.scheduled(), key.id());
    this.kvStore.put(sid, value);
    return null;
  }

  public void close() {}

}