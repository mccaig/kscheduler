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

  private static Instant SCHEDULED = Instant.EPOCH;
  private static String DESTINATION = "destination.topic";
  private static String ID = "123456";
  private static Instant CREATED = Instant.EPOCH.minus(Duration.ofDays(1));
  private static Instant EXPIRES = Instant.EPOCH.plus(Duration.ofDays(1));
  private static Instant PRODUCED = Instant.EPOCH.minus(Duration.ofSeconds(5));
  private static ScheduledRecordMetadataDeserializer DESERIALIZER = new ScheduledRecordMetadataDeserializer();

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
  public void fromProto() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, PRODUCED);
    var proto = srmpBuilder.build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(proto);
    assertEquals(expected, fromProto);
  }

  @Test
  public void fromProto_no_id() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, null, CREATED, EXPIRES, PRODUCED);
    var proto = srmpBuilder.clearId().build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(proto);
    assertEquals(expected, fromProto);
  }

  @Test
  public void fromProto_no_created() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, null, EXPIRES, PRODUCED);
    var proto = srmpBuilder.clearCreated().build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(proto);
    assertEquals(expected, fromProto);
  }

  @Test
  public void fromProto_no_expires() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, null, PRODUCED);
    var proto = srmpBuilder.clearExpires().build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(proto);
    assertEquals(expected, fromProto);
  }

  @Test
  public void fromProto_no_produced() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, null);
    var proto = srmpBuilder.clearProduced().build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(proto);
    assertEquals(expected, fromProto);
  }

  @Test
  public void fromProto_no_scheduled_throws() {
    var proto = srmpBuilder.clearScheduled().build();
    assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromProto(proto));
  }

  @Test
  public void fromProto_no_destination_throws() {
    var proto = srmpBuilder.clearDestination().build();
    assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromProto(proto));
  }

  @Test
  public void fromProto_null_returns_null() {
    assertEquals(null, ScheduledRecordMetadataDeserializer.fromProto(null));
  }

  @Test
  public void fromProto_no_fields_throws() {
    var proto = Protos.ScheduledRecordMetadata.getDefaultInstance();
    assertThrows(SerializationException.class, () -> ScheduledRecordMetadataDeserializer.fromProto(proto));
  }

  @Test
  public void desserialize() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, PRODUCED);
    var bytes = srmpBuilder.build().toByteArray();
    var fromBytes = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, fromBytes);
  }

  @Test
  public void desserialize_no_id() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, null, CREATED, EXPIRES, PRODUCED);
    var bytes = srmpBuilder.clearId().build().toByteArray();
    var fromBytes = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, fromBytes);
  }

  @Test
  public void desserialize_no_created() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, null, EXPIRES, PRODUCED);
    var bytes = srmpBuilder.clearCreated().build().toByteArray();
    var fromBytes = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, fromBytes);
  }

  @Test
  public void desserialize_no_expires() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, null, PRODUCED);
    var bytes = srmpBuilder.clearExpires().build().toByteArray();
    var fromBytes = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, fromBytes);
  }

  @Test
  public void desserialize_no_produced() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, null);
    var bytes = srmpBuilder.clearProduced().build().toByteArray();
    var fromBytes = DESERIALIZER.deserialize(null, bytes);
    assertEquals(expected, fromBytes);
  }

  @Test
  public void desserialize_no_scheduled_throws() {
    var bytes = srmpBuilder.clearScheduled().build().toByteArray();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void desserialize_no_destination_throws() {
    var bytes = srmpBuilder.clearDestination().build().toByteArray();
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void desserialize_null_returns_null() {
    var fromBytes = DESERIALIZER.deserialize(null, null);
    assertEquals(null, fromBytes);
  }

  @Test
  public void desserialize_empty_throws() {
    var bytes = new byte[0];
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, bytes));
  }

  @Test
  public void desserialize_junk_throws() {
    assertThrows(SerializationException.class, () -> DESERIALIZER.deserialize(null, new byte[] {0}));
  }


}