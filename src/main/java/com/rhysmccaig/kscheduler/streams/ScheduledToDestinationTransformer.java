package com.rhysmccaig.kscheduler.streams;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.Objects;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.TopicSettings;
import com.rhysmccaig.kscheduler.util.HeaderUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScheduledToDestinationTransformer 
    implements Transformer<ScheduledRecordMetadata, ScheduledRecord, KeyValue<Bytes, Bytes>> {
  
  private static Logger logger = LoggerFactory.getLogger(ScheduledToDestinationTransformer.class);

  private String topicSettingsStateStoreName;
  private ReadOnlyKeyValueStore<String, TopicSettings> topicSettingsStore;
  private ProcessorContext context;

  public ScheduledToDestinationTransformer(String topicSettingsStateStoreName) {
    this.topicSettingsStateStoreName = topicSettingsStateStoreName;
  }

  public ScheduledToDestinationTransformer() {
    this(null);
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    if (topicSettingsStateStoreName != null) {
      this.topicSettingsStore = (ReadOnlyKeyValueStore<String, TopicSettings>) context.getStateStore(topicSettingsStateStoreName);
    }
  }

  /**
   * Transforms records back into the pre-scheduled key and payload.
   */
  public KeyValue<Bytes, Bytes> transform(ScheduledRecordMetadata metadata, ScheduledRecord record) {
    if (context.headers() == null) {
      // if the headers are null then we are probably directly attached to a stateful processor
      // Unfortunately streams doesnt have a way to set
      logger.warn("Dropping record: Unable to set destination header. Route events via topic first.");
    } else {
      record.headers().forEach(header -> context.headers().add(header));
      HeaderUtils.stripKschedulerHeaders(context.headers());
      var topic = metadata.destination();
      if (schedulingIsDisabled(topic)) {
        var errorMessage = "Scheduled record delivery is disabled for the topic: " + topic;
        context.headers().add(
            HeaderUtils.KSCHEDULER_ERROR_HEADER_KEY, 
            errorMessage.getBytes(UTF_8));
      } else {
        context.headers().add(
          HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, 
          topic.getBytes(UTF_8));
      }
      context.headers().add(
          HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY, 
          metadata.scheduled().toString().getBytes(UTF_8));
      context.headers().add(
          HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY, 
          metadata.expires().toString().getBytes(UTF_8));
      context.headers().add(
          HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY, 
          metadata.created().toString().getBytes(UTF_8));
      context.headers().add(
          HeaderUtils.KSCHEDULER_ID_HEADER_KEY, 
          metadata.id().toString().getBytes(UTF_8));
      context.headers().add(
        HeaderUtils.KSCHEDULER_FORWARDED_AT_HEADER_KEY, 
        Instant.ofEpochMilli(context.timestamp()).toString().getBytes(UTF_8));
      final var key = new Bytes(record.key());
      final var value = new Bytes(record.value());
      context.forward(key, value);
    }
    return null;
  }

  public void close() {
    // noop
  }

  private boolean schedulingIsDisabled(String topic) {
    if (topicSettingsStore != null) {
      var settings = topicSettingsStore.get(topic);
      if (settings != null) {
        return settings.getSchedulingDisabled();
      }
    }
    return false;
  }

}