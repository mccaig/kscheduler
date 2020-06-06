package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;

import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.errors.SerializationException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataDeserializerTest {

  private ScheduledRecordMetadata srm;
  private Protos.ScheduledRecordMetadata srmp;
  private byte[] srmpb;
  private Instant scheduled;
  private String destination;
  private String id;
  private Instant created;
  private Instant expires;
  private Instant produced;

  // desserialize
  // fromBytes
  // fromProto

  @BeforeEach
  public void beforeEach() {
    scheduled = Instant.now();
    destination = "destination.topic";
    id = "123456";
    created = scheduled.minus(Duration.ofDays(1));
    expires = scheduled.plus(Duration.ofDays(1));
    produced = scheduled.minus(Duration.ofSeconds(5));
    srm = new ScheduledRecordMetadata(scheduled, destination, id, created, expires, produced);
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setDestination(destination)
        .setId(id)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
  }

  @Test
  public void fromProto() {
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(srmp);
    assertEquals(srm, fromProto);
  }

  @Test
  public void fromProto_no_id() {
    srm = new ScheduledRecordMetadata(scheduled, destination, null, created, expires, produced);
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setDestination(destination)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(srmp);
    assertEquals(srm, fromProto);
  }

  @Test
  public void fromProto_no_created() {
    srm = new ScheduledRecordMetadata(scheduled, destination, id, null, expires, produced);
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setDestination(destination)
        .setId(id)
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(srmp);
    assertEquals(srm, fromProto);
  }

  @Test
  public void fromProto_no_expires() {
    srm = new ScheduledRecordMetadata(scheduled, destination, id, created, null, produced);
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setDestination(destination)
        .setId(id)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(srmp);
    assertEquals(srm, fromProto);
  }

  @Test
  public void fromProto_no_produced() {
    srm = new ScheduledRecordMetadata(scheduled, destination, id, created, expires, null);
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setDestination(destination)
        .setId(id)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(srmp);
    assertEquals(srm, fromProto);
  }

  @Test
  public void fromProto_null_returns_null() {
    assertEquals(null, ScheduledRecordMetadataDeserializer.fromProto(null));
  }

  @Test
  public void fromProto_no_fields_throws() {
    srmp = Protos.ScheduledRecordMetadata.getDefaultInstance();
    assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromProto(srmp));
  }

  @Test
  public void fromProto_no_scheduled_throws() {
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setDestination(destination)
        .setId(id)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
        assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromProto(srmp));
  }

  @Test
  public void fromProto_no_destination_throws() {
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setId(id)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
        assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromProto(srmp));
  }

  @Test
  public void fromBytes() {
    srmpb = srmp.toByteArray();
    var fromBytes = ScheduledRecordMetadataDeserializer.fromBytes(srmpb);
    assertEquals(srm, fromBytes);
  }

  @Test
  public void fromBytes_null_returns_null() {
    srmpb = null;
    assertEquals(null, ScheduledRecordMetadataDeserializer.fromBytes(srmpb));
  }

  @Test
  public void fromBytes_empty_throws() {
    srmpb = new byte[0];
    assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromBytes(srmpb));
  }

  @Test
  public void fromBytes_junk_throws() {
    srmpb = new byte[] {0};
    assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromBytes(srmpb));
  }

  @Test
  public void desserialize() {
    srmpb = srmp.toByteArray();
    ScheduledRecordMetadata fromBytes;
    try (var deserializer = new ScheduledRecordMetadataDeserializer()) {
      fromBytes = deserializer.deserialize(null, srmpb);
    }
    assertEquals(srm, fromBytes);
  }

  @Test
  public void desserialize_null_returns_null() {
    ScheduledRecordMetadata fromBytes;
    try (var deserializer = new ScheduledRecordMetadataDeserializer()) {
      fromBytes = deserializer.deserialize(null, null);
    }
    assertEquals(null, fromBytes);
  }

  @Test
  public void desserialize_empty_throws() {
    try (var deserializer = new ScheduledRecordMetadataDeserializer()) {
      assertThrows(SerializationException.class, () -> deserializer.deserialize(null, new byte[0]));
    }
  }

  @Test
  public void desserialize_junk_throws() {
    try (var deserializer = new ScheduledRecordMetadataDeserializer()) {
      assertThrows(SerializationException.class, () -> deserializer.deserialize(null, new byte[] {0}));
    }
  }


}