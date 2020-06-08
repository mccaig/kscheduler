package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;

import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataSerializerTest {

  private static Instant SCHEDULED = Instant.EPOCH;
  private static String DESTINATION = "destination.topic";
  private static String ID = "123456";
  private static Instant CREATED = Instant.EPOCH.minus(Duration.ofDays(1));
  private static Instant EXPIRES = Instant.EPOCH.plus(Duration.ofDays(1));
  private static Instant PRODUCED = Instant.EPOCH.minus(Duration.ofSeconds(5));
  private static ScheduledRecordMetadataSerializer SERIALIZER = new ScheduledRecordMetadataSerializer();

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
  }

  @Test
  public void toProto() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, PRODUCED);
    var expected = srmpBuilder.build();
    var proto = ScheduledRecordMetadataSerializer.toProto(metadata);
    assertEquals(expected, proto);
  }

  @Test
  public void toProto_no_id() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, null, CREATED, EXPIRES, PRODUCED);
    var expected = srmpBuilder.clearId().build();
    var proto = ScheduledRecordMetadataSerializer.toProto(metadata);
    assertEquals(expected, proto);
  }

  @Test
  public void toProto_no_created() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, null, EXPIRES, PRODUCED);
    var expected = srmpBuilder.clearCreated().build();
    var proto = ScheduledRecordMetadataSerializer.toProto(metadata);
    assertEquals(expected, proto);
  }

  @Test
  public void toProto_no_expires() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, null, PRODUCED);
    var expected = srmpBuilder.clearExpires().build();
    var proto = ScheduledRecordMetadataSerializer.toProto(metadata);
    assertEquals(expected, proto);
  }

  @Test
  public void toProto_no_produced() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, null);
    var expected = srmpBuilder.clearProduced().build();
    var proto = ScheduledRecordMetadataSerializer.toProto(metadata);
    assertEquals(expected, proto);
  }

  @Test
  public void toProto_null_returns_null() {
    var proto = ScheduledRecordMetadataSerializer.toProto(null);
    assertEquals(null, proto);
  }

  @Test
  public void serialize() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, PRODUCED);
    var expected = srmpBuilder.build().toByteArray();
    var bytes = SERIALIZER.serialize(null, metadata);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_no_id() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, null, CREATED, EXPIRES, PRODUCED);
    var expected = srmpBuilder.clearId().build().toByteArray();
    var bytes = SERIALIZER.serialize(null, metadata);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_no_created() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID,  null, EXPIRES, PRODUCED);
    var expected = srmpBuilder.clearCreated().build().toByteArray();
    var bytes = SERIALIZER.serialize(null, metadata);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_no_expires() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID,  CREATED, null, PRODUCED);
    var expected = srmpBuilder.clearExpires().build().toByteArray();
    var bytes = SERIALIZER.serialize(null, metadata);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_no_produced() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID,  CREATED, EXPIRES, null);
    var expected = srmpBuilder.clearProduced().build().toByteArray();
    var bytes = SERIALIZER.serialize(null, metadata);
    assertArrayEquals(expected, bytes);
  }

  @Test
  public void serialize_null_returns_null() {
    var bytes = SERIALIZER.serialize(null, null);
    assertArrayEquals(null, bytes);
  }



}