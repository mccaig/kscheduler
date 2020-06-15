package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class ScheduledRecordSerializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH;
  private static String DESTINATION = "destination";
  private static String ID = "1234";
  private static Instant CREATED = Instant.EPOCH.minus(Duration.ofDays(1));
  private static Instant EXPIRES = Instant.EPOCH.plus(Duration.ofDays(1));
  private static Instant PRODUCED = Instant.EPOCH.plus(Duration.ofMinutes(1));
  private static String HEADER_KEY = "hello";
  private static byte[] HEADER_VALUE = "world".getBytes(StandardCharsets.UTF_8);
  private static byte[] KEY = "1234".getBytes(StandardCharsets.UTF_8);
  private static byte[] VALUE = "abcd".getBytes(StandardCharsets.UTF_8);
  private static Headers HEADERS = new RecordHeaders().add(new RecordHeader("hello", "world".getBytes(StandardCharsets.UTF_8)));
  private static ScheduledRecordSerializer SERIALIZER = new ScheduledRecordSerializer();

  private Protos.ScheduledRecord.Builder srpBuilder;
  private ScheduledRecordMetadata srm = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, PRODUCED);
  private Protos.ScheduledRecordMetadata.Builder srmpBuilder;


  @BeforeEach
  public void beforeEach() {
    srmpBuilder = Protos.ScheduledRecordMetadata.newBuilder()
            .setScheduled(Timestamp.newBuilder().setSeconds(SCHEDULED.getEpochSecond()).setNanos(SCHEDULED.getNano()))
            .setDestination(DESTINATION)
            .setId(ID)
            .setCreated(Timestamp.newBuilder().setSeconds(CREATED.getEpochSecond()).setNanos(CREATED.getNano()))
            .setExpires(Timestamp.newBuilder().setSeconds(EXPIRES.getEpochSecond()).setNanos(EXPIRES.getNano()))
            .setProduced(Timestamp.newBuilder().setSeconds(PRODUCED.getEpochSecond()).setNanos(PRODUCED.getNano()));
    srpBuilder = Protos.ScheduledRecord.newBuilder()
        .setMetadata(srmpBuilder)
        .setKey(ByteString.copyFrom(KEY))
        .setValue(ByteString.copyFrom(VALUE))
        .addHeaders(Protos.ScheduledRecordHeader.newBuilder()
            .setKey(HEADER_KEY)
            .setValue(ByteString.copyFrom(HEADER_VALUE)));
  }


  @Test
  public void serialize() {
    var record = new ScheduledRecord(srm, KEY, VALUE, HEADERS);
    var expected = srpBuilder.build().toByteArray();
    var bytes = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_no_key() {
    var record  = new ScheduledRecord(srm, null, VALUE, HEADERS);
    var expected = srpBuilder.clearKey().build().toByteArray();
    var bytes = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_no_value() {
    var record  = new ScheduledRecord(srm, KEY, null, HEADERS);
    var expected = srpBuilder.clearValue().build().toByteArray();
    var bytes = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_no_headers() {
    var record = new ScheduledRecord(srm, KEY, VALUE, null);
    var expected = srpBuilder.clearHeaders().build().toByteArray();
    var bytes = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_null_returns_null() {
    byte[] expected = null;
    var bytes = SERIALIZER.serialize(null, null);
    assertArrayEquals(expected, bytes);
  }

}