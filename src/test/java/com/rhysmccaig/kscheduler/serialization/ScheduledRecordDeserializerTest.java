package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledRecordDeserializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH;
  private static String DESTINATION = "destination";
  private static String HEADER_KEY = "hello";
  private static byte[] HEADER_VALUE = "world".getBytes(StandardCharsets.UTF_8);
  private static ScheduledRecordMetadata METADATA = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, null, null, null, null);
  private static byte[] KEY = "1234".getBytes(StandardCharsets.UTF_8);
  private static byte[] VALUE = "abcd".getBytes(StandardCharsets.UTF_8);
  private static Headers HEADERS = new RecordHeaders().add(new RecordHeader(HEADER_KEY, HEADER_VALUE));

  private static ScheduledRecordDeserializer DESERIALIZER = new ScheduledRecordDeserializer();

  private Protos.ScheduledRecord.Builder srpBuilder;

  @BeforeEach
  public void beforeEach() {
    srpBuilder = Protos.ScheduledRecord.newBuilder()
        .setMetadata(Protos.ScheduledRecordMetadata.newBuilder()
            .setScheduled(Timestamp.newBuilder().setSeconds(SCHEDULED.getEpochSecond()).setNanos(SCHEDULED.getNano()))
            .setDestination(DESTINATION))
        .setKey(ByteString.copyFrom(KEY))
        .setValue(ByteString.copyFrom(VALUE))
        .addHeaders(Protos.ScheduledRecordHeader.newBuilder()
            .setKey(HEADER_KEY)
            .setValue(ByteString.copyFrom(HEADER_VALUE)));
  }

  @Test
  public void deserialize() {
    var expected = new ScheduledRecord(METADATA, KEY, VALUE, HEADERS);
    var bytes = srpBuilder.build().toByteArray();
    assertEquals(expected, DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_no_key() {
    var expected = new ScheduledRecord(METADATA, null, VALUE, HEADERS);
    var bytes = srpBuilder.clearKey().build().toByteArray();
    assertEquals(expected, DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_no_value() {
    var expected = new ScheduledRecord(METADATA, KEY, null, HEADERS);
    var bytes = srpBuilder.clearValue().build().toByteArray();
    assertEquals(expected, DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_no_headers() {
    var record = new ScheduledRecord(METADATA, KEY, VALUE, null);
    var bytes = srpBuilder.clearHeaders().build().toByteArray();
    assertEquals(record, DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void deserialize_no_metadata_throws() {
    var bytes = srpBuilder.clearMetadata().build().toByteArray();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void desserialize_empty_throws() {
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, new byte[0]));
  }

  @Test
  public void desserialize_junk_throws() {
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, new byte[] {0}));
  }

}