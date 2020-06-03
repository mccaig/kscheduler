package com.rhysmccaig.kscheduler.streams;

import com.rhysmccaig.kscheduler.model.ScheduledId;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.TopicPartitionOffset;
import com.rhysmccaig.kscheduler.util.HeaderUtils;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SourceToScheduledTransformer
    implements Transformer<Bytes, Bytes, KeyValue<ScheduledRecordMetadata, ScheduledRecord>> {
  
  static final Logger logger = LogManager.getLogger(SourceToScheduledTransformer.class); 

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public KeyValue<ScheduledRecordMetadata, ScheduledRecord> transform(Bytes key, Bytes value) {
    var headers = this.context.headers();
    var metadata = HeaderUtils.extractMetadata(headers, true);
    KeyValue<ScheduledRecordMetadata, ScheduledRecord> transformed = null;
    if (metadata != null && metadata.scheduled() != null && metadata.id() != null) {
      // Metadata header already exists
      transformed = new KeyValue<ScheduledRecordMetadata, ScheduledRecord>(metadata, new ScheduledRecord(metadata, key.get(), value.get(), headers));
    } else {
      // Fallback to non metadata headers
      if (logger.isDebugEnabled()) {
        var tpo = new TopicPartitionOffset(context.topic(), context.partition(), context.offset());
        logger.debug("No scheduler metadata found for message: {}, dropping message.", tpo);
      }
    }
    return transformed;
  }


  @Override
  public void close() {}
  
}