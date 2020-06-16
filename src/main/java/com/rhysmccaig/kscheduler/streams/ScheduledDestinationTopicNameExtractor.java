package com.rhysmccaig.kscheduler.streams;

import java.nio.charset.StandardCharsets;

import com.rhysmccaig.kscheduler.util.HeaderUtils;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

public class ScheduledDestinationTopicNameExtractor implements TopicNameExtractor<Bytes, Bytes> {

  @Override
  public String extract(Bytes key, Bytes value, RecordContext recordContext) {
    var destinationHeader = recordContext.headers().lastHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY);
    var topic = (destinationHeader == null) ? null : new String(destinationHeader.value(), StandardCharsets.UTF_8);
    return topic;
  }
  
  
}