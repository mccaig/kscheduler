package com.rhysmccaig.kscheduler.processor;

import java.time.Duration;
import java.util.Objects;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;


public class ScheduleProcessor implements Processor<byte[], byte[]> {
  static final Logger logger = LogManager.getLogger(ScheduleProcessor.class); 

  // How often we scan the records database for records that are ready to be forwarded.
  public static final Duration DEFAULT_PUNCTUATE_DURATION = Duration.ofSeconds(5);
  public static final String STATE_STORE_NAME = "kscheduler-scheduled";

  private ProcessorContext context;
  private KeyValueStore<ScheduledRecordMetadata, ScheduledRecord> kvStore;
  private final Duration punctuateSchedule;

  public ScheduleProcessor(Duration punctuateSchedule) {
    Objects.requireNonNull(punctuateSchedule);
    this.punctuateSchedule = punctuateSchedule;
  }

  public ScheduleProcessor() {
    this(DEFAULT_PUNCTUATE_DURATION);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    kvStore = (KeyValueStore<ScheduledRecordMetadata, ScheduledRecord>) context.getStateStore(STATE_STORE_NAME);

    // schedule a punctuate() method every second based on wall-clock time
    this.context.schedule(punctuateSchedule, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
      KeyValueIterator<ScheduledRecordMetadata, ScheduledRecord> iter = this.kvStore.range(null, null);
      while (iter.hasNext()) {
          KeyValue<ScheduledRecordMetadata, ScheduledRecord> entry = iter.next();
          context.forward(entry.key, entry.value);
          this.kvStore.delete(entry.key);
      }
      iter.close();
      // commit the current processing progress
      context.commit();
  });

  }

  public void process(byte[] key, byte[] value) {

  }

  public void close() {

  }

}