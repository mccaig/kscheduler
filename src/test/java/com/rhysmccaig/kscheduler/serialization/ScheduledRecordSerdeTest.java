package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

public class ScheduledRecordSerdeTest {
  
  private static Serde<ScheduledRecord> SERDE = new ScheduledRecordSerde();
  private static ScheduledRecordMetadata METADATA = new ScheduledRecordMetadata(
    Instant.EPOCH, "destination", "1234", Instant.MIN, Instant.MAX, Instant.EPOCH.minus(Duration.ofSeconds(5)));
  private static byte[] KEY = "123456".getBytes(StandardCharsets.UTF_8);
  private static byte[] VALUE = "abcd".getBytes(StandardCharsets.UTF_8);
  private static Headers HEADERS = new RecordHeaders().add(new RecordHeader("7890", "WXYZ".getBytes(StandardCharsets.UTF_8)));
  private static Deserializer<ScheduledRecord> deserializer = SERDE.deserializer();
  private static Serializer<ScheduledRecord> serializer = SERDE.serializer();


  @Test
  public void roundtrip() {
    var expected = new ScheduledRecord(METADATA, KEY, VALUE, HEADERS);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_key() {
    var expected = new ScheduledRecord(METADATA, null, VALUE, HEADERS);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_value() {
    var expected = new ScheduledRecord(METADATA, KEY, null, HEADERS);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_headers() {
    var expected = new ScheduledRecord(METADATA, KEY, VALUE, null);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }



}