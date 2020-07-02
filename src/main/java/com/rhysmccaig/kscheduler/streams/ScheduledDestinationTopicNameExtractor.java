package com.rhysmccaig.kscheduler.streams;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.rhysmccaig.kscheduler.util.HeaderUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

public class ScheduledDestinationTopicNameExtractor implements TopicNameExtractor<Bytes, Bytes> {

  private String deadLetterQueueTopicName;

  public ScheduledDestinationTopicNameExtractor(String deadLetterQueueTopicName) {
    this.deadLetterQueueTopicName = deadLetterQueueTopicName;
  }
  @Override
  public String extract(Bytes key, Bytes value, RecordContext recordContext) {
    var destinationHeader = recordContext.headers().lastHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY);
    return (destinationHeader == null) ? deadLetterQueueTopicName : new String(destinationHeader.value(), UTF_8);
  }
  
  
}