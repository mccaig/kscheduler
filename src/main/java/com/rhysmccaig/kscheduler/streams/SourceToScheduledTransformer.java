package com.rhysmccaig.kscheduler.streams;

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
    if (headers == null 
      || headers.lastHeader(HeaderUtils.KSCHEDULER_FORWARDED_AT_HEADER_KEY) != null) {
        // if there are no headers, or this record has already been forwarded
        // then drop the record
        return null;
      }
    var metadata = HeaderUtils.extractMetadata(headers, true);
    if (metadata != null) {
      var record = new ScheduledRecord(metadata, key.get(), value.get(), headers);
      context.forward(metadata, record);
    } else if (logger.isDebugEnabled()) {
      var tpo = new TopicPartitionOffset(context.topic(), context.partition(), context.offset());
      logger.debug("No scheduler metadata found for message: {}, dropping message.", tpo);
    }
    return null; 
  }


  @Override
  public void close() {
    // noop
  }
  
}