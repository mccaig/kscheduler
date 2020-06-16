package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import com.google.protobuf.ByteString;
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
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";
  private static ScheduledRecordMetadata METADATA = new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);
  private static ScheduledRecordMetadataSerializer METADATAA_SERIALIZER = new ScheduledRecordMetadataSerializer();
  private static byte[] METADATA_BYTES = METADATAA_SERIALIZER.serialize(METADATA);

  private static String HEADER_KEY = "hello";
  private static byte[] HEADER_VALUE = "world".getBytes(StandardCharsets.UTF_8);
  private static byte[] KEY = "1234".getBytes(StandardCharsets.UTF_8);
  private static byte[] VALUE = "abcd".getBytes(StandardCharsets.UTF_8);
  private static Headers HEADERS = new RecordHeaders().add(new RecordHeader(HEADER_KEY, HEADER_VALUE));

  private static ScheduledRecordDeserializer DESERIALIZER = new ScheduledRecordDeserializer();

  private Protos.ScheduledRecord.Builder srpBuilder;

  @BeforeEach
  public void beforeEach() {
    srpBuilder = Protos.ScheduledRecord.newBuilder()
        .setMetadata(ByteString.copyFrom(METADATA_BYTES))
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