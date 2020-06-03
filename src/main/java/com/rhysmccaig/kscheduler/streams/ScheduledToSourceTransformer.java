package com.rhysmccaig.kscheduler.streams;

import java.nio.charset.StandardCharsets;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.util.HeaderUtils;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ScheduledToSourceTransformer implements Transformer<ScheduledRecordMetadata, ScheduledRecord, KeyValue<Bytes, Bytes>> {
  
  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  public KeyValue<Bytes, Bytes> transform(ScheduledRecordMetadata key, ScheduledRecord value) {
    context.headers().add(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, key.destination().getBytes(StandardCharsets.UTF_8));
    final var newKey = new Bytes(value.key());
    final var newValue = new Bytes(value.value());
    return new KeyValue<Bytes, Bytes>(newKey, newValue);
  }
  public void close() {}

}