package com.rhysmccaig.kscheduler.serdes;

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

  private ScheduledRecord sr;
  private Protos.ScheduledRecord srp;
  private byte[] srpb;
  private ScheduledRecordMetadata srm;
  private byte[] key;
  private byte[] value;
  private Headers headers;
  private ScheduledRecordDeserializer deserializer;

  @BeforeEach
  public void beforeEach() {
    srm = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, null, null, null, null);
    key = "1234".getBytes(StandardCharsets.UTF_8);
    value = "abcd".getBytes(StandardCharsets.UTF_8);
    headers = new RecordHeaders().add(new RecordHeader(HEADER_KEY, HEADER_VALUE));
    sr = new ScheduledRecord(srm, key, value, headers);
    srp = Protos.ScheduledRecord.newBuilder()
        .setMetadata(Protos.ScheduledRecordMetadata.newBuilder()
            .setScheduled(Timestamp.newBuilder().setSeconds(SCHEDULED.getEpochSecond()).setNanos(SCHEDULED.getNano()))
            .setDestination(DESTINATION))
        .setKey(ByteString.copyFrom(key))
        .setValue(ByteString.copyFrom(value))
        .addHeaders(Protos.ScheduledRecordHeader.newBuilder()
            .setKey(HEADER_KEY)
            .setValue(ByteString.copyFrom(HEADER_VALUE)))
        .build();
    srpb = srp.toByteArray();
    deserializer = new ScheduledRecordDeserializer();
  }

  @Test
  public void deserialize() {
    assertEquals(sr, deserializer.deserialize(null, srpb));
  }

  @Test
  public void deserialize_no_key() {
    sr = new ScheduledRecord(srm, null, value, headers);
    srpb = Protos.ScheduledRecord.newBuilder()
      .setMetadata(Protos.ScheduledRecordMetadata.newBuilder()
          .setScheduled(Timestamp.newBuilder().setSeconds(SCHEDULED.getEpochSecond()).setNanos(SCHEDULED.getNano()))
          .setDestination(DESTINATION))
      .setValue(ByteString.copyFrom(value))
      .addHeaders(Protos.ScheduledRecordHeader.newBuilder()
          .setKey(HEADER_KEY)
          .setValue(ByteString.copyFrom(HEADER_VALUE)))
      .build().toByteArray();
    assertEquals(sr, deserializer.deserialize(null, srpb));
  }

  @Test
  public void deserialize_no_value() {
    sr = new ScheduledRecord(srm, key, null, headers);
    srpb = Protos.ScheduledRecord.newBuilder()
      .setMetadata(Protos.ScheduledRecordMetadata.newBuilder()
          .setScheduled(Timestamp.newBuilder().setSeconds(SCHEDULED.getEpochSecond()).setNanos(SCHEDULED.getNano()))
          .setDestination(DESTINATION))
      .setKey(ByteString.copyFrom(key))
      .addHeaders(Protos.ScheduledRecordHeader.newBuilder()
          .setKey(HEADER_KEY)
          .setValue(ByteString.copyFrom(HEADER_VALUE)))
      .build().toByteArray();
    assertEquals(sr, deserializer.deserialize(null, srpb));
  }

  @Test
  public void deserialize_no_headers() {
    sr = new ScheduledRecord(srm, key, value, null);
    srpb = Protos.ScheduledRecord.newBuilder()
      .setMetadata(Protos.ScheduledRecordMetadata.newBuilder()
          .setScheduled(Timestamp.newBuilder().setSeconds(SCHEDULED.getEpochSecond()).setNanos(SCHEDULED.getNano()))
          .setDestination(DESTINATION))
      .setKey(ByteString.copyFrom(key))
      .setValue(ByteString.copyFrom(value))
      .build().toByteArray();
    assertEquals(sr, deserializer.deserialize(null, srpb));
  }

  @Test
  public void deserialize_no_metadata_throws() {
    srpb = Protos.ScheduledRecord.newBuilder()
      .setKey(ByteString.copyFrom(key))
      .setValue(ByteString.copyFrom(value))
      .addHeaders(Protos.ScheduledRecordHeader.newBuilder()
        .setKey(HEADER_KEY)
        .setValue(ByteString.copyFrom(HEADER_VALUE)))
      .build().toByteArray();
      assertThrows(SerializationException.class, () -> deserializer.deserialize(null, srpb));
  }

  @Test
  public void desserialize_empty_throws() {
    try (var deserializer = new ScheduledRecordDeserializer()) {
      assertThrows(SerializationException.class, () -> deserializer.deserialize(null, new byte[0]));
    }
  }

  @Test
  public void desserialize_junk_throws() {
    try (var deserializer = new ScheduledRecordDeserializer()) {
      assertThrows(SerializationException.class, () -> deserializer.deserialize(null, new byte[] {0}));
    }
  }

}