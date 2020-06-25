package com.rhysmccaig.kscheduler.streams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledToSourceTransformerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH;
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";
  private static ScheduledRecordMetadata METADATA = 
      new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);

  private static byte[] KEY = "Hello".getBytes(UTF_8);
  private static byte[] VALUE = "World!".getBytes(UTF_8);

  private ScheduledToSourceTransformer transformer;
  private MockProcessorContext context;

  /**
   * Test setup.
   */
  @BeforeEach
  public void setup() {
    context = new MockProcessorContext();
    transformer = new ScheduledToSourceTransformer();
    transformer.init(context);
  }

  @AfterEach
  public void teardown() {
    transformer.close();
  }

  @Test
  public void transforms_message() {
    context.setHeaders(new RecordHeaders());
    var header = new RecordHeader("foo", "bar".getBytes(UTF_8));
    var headers = new RecordHeaders().add(header);
    var record = new ScheduledRecord(METADATA, KEY, VALUE, headers);
    var expected = new KeyValue<>(Bytes.wrap(KEY), Bytes.wrap(VALUE));

    var returned = transformer.transform(METADATA, record);
    var forwarded = context.forwarded().iterator();
    var result = forwarded.next().keyValue();
  
    assertNull(returned);
    assertEquals(expected, result);
  }

  @Test
  public void drops_message_when_headers_cant_be_set() {
    var header = new RecordHeader("foo", "bar".getBytes(UTF_8));
    var headers = new RecordHeaders().add(header);
    var record = new ScheduledRecord(METADATA, KEY, VALUE, headers);

    var returned = transformer.transform(METADATA, record);
    var forwarded = context.forwarded().iterator();
  
    assertNull(returned);
    assertFalse(forwarded.hasNext());
  }


}