package com.rhysmccaig.kscheduler.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerializer;
import com.rhysmccaig.kscheduler.util.HeaderUtils;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SourceToScheduledTransformerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH;
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";
  private static ScheduledRecordMetadata METADATA = new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);
  private static ScheduledRecordMetadataSerializer METADATA_SERIALIZER = new ScheduledRecordMetadataSerializer();

  private SourceToScheduledTransformer transformer;
  private MockProcessorContext context;

  @BeforeEach
  public void setup() {
    context = new MockProcessorContext();
    transformer = new SourceToScheduledTransformer();
    transformer.init(context);
  }

  @AfterEach
  public void teardown() {
    transformer.close();
  }

  @Test
  public void transforms_message_with_metadata() {
    var key = "Hello".getBytes(StandardCharsets.UTF_8);
    var value = "world!".getBytes(StandardCharsets.UTF_8);
    var bytes = METADATA_SERIALIZER.serialize(METADATA);
    var header = new RecordHeader(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, bytes);
    var headers = new RecordHeaders().add(header);
    var record = new ScheduledRecord(METADATA, key, value, headers);
    var expected = new KeyValue<>(METADATA, record);
    context.setHeaders(headers);

    var returned = transformer.transform(Bytes.wrap(key), Bytes.wrap(value));
    var forwarded = context.forwarded().iterator();
    var result = forwarded.next().keyValue();
    
    assertNull(returned);
    assertEquals(expected, result);
  }

  @Test
  public void drops_message_without_metadata() {
    var key = "Hello".getBytes(StandardCharsets.UTF_8);
    var value = "world!".getBytes(StandardCharsets.UTF_8);
    var header = new RecordHeader("foo", "bar".getBytes(StandardCharsets.UTF_8));
    var headers = new RecordHeaders().add(header);
    context.setHeaders(headers);
    context.setTopic("baz");
    context.setPartition(0);
    context.setOffset(0);

    var returned = transformer.transform(Bytes.wrap(key), Bytes.wrap(value));
    var forwarded = context.forwarded().iterator();
    
    assertNull(returned);
    assertFalse(forwarded.hasNext());
  }

  
}