package com.rhysmccaig.kscheduler.streams;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.util.HeaderUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ScheduledToSourceTransformer 
    implements Transformer<ScheduledRecordMetadata, ScheduledRecord, KeyValue<Bytes, Bytes>> {
  
  private static Logger logger = LogManager.getLogger(ScheduledToSourceTransformer.class);

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  /**
   * Transforms records back into the pre-scheduled key and payload.
   */
  @Override
  public KeyValue<Bytes, Bytes> transform(ScheduledRecordMetadata key, ScheduledRecord value) {
    if (context.headers() == null) {
      // if the headers are null then we are probably directly attached to a stateful processor
      // Unfortunately streams doesnt have a way to set
      logger.warn("Dropping record: Unable to set destination header. Route events via topic first.");
    } else {
      value.headers().forEach(header -> context.headers().add(header));
      context.headers()
          .add(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, key.destination().getBytes(UTF_8));
      final var newKey = new Bytes(value.key());
      final var newValue = new Bytes(value.value());
      context.forward(newKey, newValue);
    }
    return null;
  }

  @Override
  public void close() {
    // noop
  }

}