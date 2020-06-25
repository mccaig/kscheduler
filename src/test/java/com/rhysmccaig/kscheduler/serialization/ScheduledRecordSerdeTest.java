package com.rhysmccaig.kscheduler.serialization;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

public class ScheduledRecordSerdeTest {
  
  private static Serde<ScheduledRecord> SERDE = new ScheduledRecordSerde();
  private static Deserializer<ScheduledRecord> DESERIALIZER = SERDE.deserializer();
  private static Serializer<ScheduledRecord> SERIALIZER = SERDE.serializer();

  private static Instant SCHEDULED = Instant.EPOCH;
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";
  private static ScheduledRecordMetadata METADATA = 
      new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);

  private static byte[] KEY = "123456".getBytes(UTF_8);
  private static byte[] VALUE = "abcd".getBytes(UTF_8);
  private static Headers HEADERS = new RecordHeaders().add(new RecordHeader("7890", "WXYZ".getBytes(UTF_8)));

  @Test
  public void roundtrip() {
    var expected = new ScheduledRecord(METADATA, KEY, VALUE, HEADERS);
    var actual = DESERIALIZER.deserialize(null, SERIALIZER.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_key() {
    var expected = new ScheduledRecord(METADATA, null, VALUE, HEADERS);
    var actual = DESERIALIZER.deserialize(null, SERIALIZER.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_value() {
    var expected = new ScheduledRecord(METADATA, KEY, null, HEADERS);
    var actual = DESERIALIZER.deserialize(null, SERIALIZER.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_headers() {
    var expected = new ScheduledRecord(METADATA, KEY, VALUE, null);
    var actual = DESERIALIZER.deserialize(null, SERIALIZER.serialize(null, expected));
    assertEquals(expected, actual);
  }



}