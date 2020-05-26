package com.rhysmccaig.kscheduler.processor;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;


public class ScheduleProcessor implements Processor<ScheduledRecordMetadata, ScheduledRecord> {
  static final Logger logger = LogManager.getLogger(ScheduleProcessor.class); 

  // How often we scan the records database for records that are ready to be forwarded.
  public static final Duration DEFAULT_PUNCTUATE_INTERVAL = Duration.ofSeconds(5);
  public static final String STATE_STORE_NAME = "kscheduler-scheduled";
  public static final String PROCESSOR_NAME = "kscheduler-processor";

  private ProcessorContext context;
  private KeyValueStore<String, ScheduledRecord> kvStore;
  private final Duration punctuateSchedule;

  public ScheduleProcessor(Duration punctuateSchedule) {
    Objects.requireNonNull(punctuateSchedule);
    this.punctuateSchedule = punctuateSchedule;
  }

  public ScheduleProcessor() {
    this(DEFAULT_PUNCTUATE_INTERVAL);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    kvStore = (KeyValueStore<String, ScheduledRecord>) context.getStateStore(STATE_STORE_NAME);

    // schedule a punctuate() method every second based on wall-clock time
    this.context.schedule(punctuateSchedule, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
      var notBeforeInstant = Instant.ofEpochMilli(timestamp);
      var beforeInstant = Instant.ofEpochMilli(timestamp).plus(punctuateSchedule);
      var from = new ScheduledId(notBeforeInstant, null).toString();
      var to = new ScheduledId(beforeInstant, null).toString();
      // Cant yet define our insertion order, but by default RocksDB orders items lexigraphically
      // 
      KeyValueIterator<String, ScheduledRecord> iter = this.kvStore.range(from, to);
      while (iter.hasNext()) {
          KeyValue<String, ScheduledRecord> entry = iter.next();
          var key = ScheduledId.fromString(entry.key);
          var value = entry.value;
          assert (key.scheduled().isAfter(notBeforeInstant.minus(Duration.ofNanos(1))));
          assert (value.metadata().scheduled().isAfter(notBeforeInstant.minus(Duration.ofNanos(1))));
          assert (key.scheduled().isBefore(beforeInstant));
          assert (value.metadata().scheduled().isBefore(beforeInstant));
          context.forward(value.metadata(), value);
          this.kvStore.delete(entry.key);
      }
      iter.close();
      // commit the current processing progress?
      context.commit();
    });
  }

  /**
   * Add the record into the state store for processing
   */
  public void process(ScheduledRecordMetadata key, ScheduledRecord value) {
    this.kvStore.put(new ScheduledId(key.scheduled(), key.id()).toString(), value);
  }

  public void close() {

  }

}