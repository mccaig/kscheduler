package com.rhysmccaig.kscheduler.streams;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.TopicPartitionOffset;
import com.rhysmccaig.kscheduler.model.TopicSettings;
import com.rhysmccaig.kscheduler.util.HeaderUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceToScheduledTransformer
    implements Transformer<Bytes, Bytes, KeyValue<ScheduledRecordMetadata, ScheduledRecord>> {
  
  static final Logger logger = LoggerFactory.getLogger(SourceToScheduledTransformer.class); 

  private String topicSettingsStateStoreName;
  private ReadOnlyKeyValueStore<String, TopicSettings> topicSettingsStore;
  private TopicSettings defaultSettings;
  private ProcessorContext context;

  public SourceToScheduledTransformer(String topicSettingsStateStoreName, TopicSettings defaultTopicSettings) {
    this.topicSettingsStateStoreName = topicSettingsStateStoreName;
    this.defaultSettings = defaultTopicSettings;
  }

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
    if (metadata != null && schedulingEnabled(metadata.destination())) {
        var record = new ScheduledRecord(metadata, key.get(), value.get(), headers);
        context.forward(metadata, record);
    } else if (logger.isDebugEnabled()) {
      var tpo = new TopicPartitionOffset(context.topic(), context.partition(), context.offset());
      if (metadata != null) { 
        logger.debug(
            "Scheduling not enabled for destination topic: {}, message: {}, dropping message.", 
            metadata.destination(), 
            tpo);
      } else {
        logger.debug("No scheduler metadata found for message: {}, dropping message.", tpo);
      }
    }
    return null; 
  }


  @Override
  public void close() {
    topicSettingsStore = null;
  }

  private boolean schedulingEnabled(String topic) {
    if (topicSettingsStore != null) {
      var settings = topicSettingsStore.get(topic);
      if (settings != null) {
        // If this topic explicitly has scheduling enabled or disabled, use that setting
        return settings.getSchedulingEnabled();
      }
    }
    // If this topic does not explicitly have scheduling enabled or disabled, use the global setting
    return defaultSettings.getSchedulingEnabled();
  }
  
}