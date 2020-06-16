package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import com.google.protobuf.ByteString;
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
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";

  private static String HEADER_KEY = "hello";
  private static byte[] HEADER_VALUE = "world".getBytes(StandardCharsets.UTF_8);
  private static byte[] KEY = "1234".getBytes(StandardCharsets.UTF_8);
  private static byte[] VALUE = "abcd".getBytes(StandardCharsets.UTF_8);
  private static Headers HEADERS = new RecordHeaders().add(new RecordHeader("hello", "world".getBytes(StandardCharsets.UTF_8)));
  private static ScheduledRecordSerializer SERIALIZER = new ScheduledRecordSerializer();
  private static ScheduledRecordMetadataSerializer METADATA_SERIALIZER = new ScheduledRecordMetadataSerializer();

  private Protos.ScheduledRecord.Builder srpBuilder;
  private ScheduledRecordMetadata srm;


  @BeforeEach
  public void beforeEach() {
    srm = new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);
    srpBuilder = Protos.ScheduledRecord.newBuilder()
        .setMetadata(ByteString.copyFrom(METADATA_SERIALIZER.serialize(srm)))
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