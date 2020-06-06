package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;

import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataSerializerTest {
  

  private ScheduledRecordMetadata srm;
  private Protos.ScheduledRecordMetadata srmp;
  private byte[] srmpb;
  private Instant scheduled;
  private String destination;
  private String id;
  private Instant created;
  private Instant expires;
  private Instant produced;

  // serialize
  // toBytes
  // toProto

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
  public void toProto() {
    var toProto = ScheduledRecordMetadataSerializer.toProto(srm);
    assertEquals(srmp, toProto);
  }

  @Test
  public void toProto_no_id() {
    srm = new ScheduledRecordMetadata(scheduled, destination, null, created, expires, produced);
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setDestination(destination)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
    var toProto = ScheduledRecordMetadataSerializer.toProto(srm);
    assertEquals(srmp, toProto);
  }






}