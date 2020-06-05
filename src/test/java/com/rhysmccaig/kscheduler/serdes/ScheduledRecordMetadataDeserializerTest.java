package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Time;
import java.time.Duration;
import java.time.Instant;

import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataDeserializerTest {

  private ScheduledRecordMetadataDeserializer classUnderTest; 

  private ScheduledRecordMetadata srm;
  private Protos.ScheduledRecordMetadata srmp;


  // desserialize
  // fromBytes
  // fromProto

  @BeforeEach
  public void beforeEach() {
    classUnderTest = new ScheduledRecordMetadataDeserializer();
  }

  @Test
  public void fromProto() {
    var scheduled = Instant.now();
    var destination = "destination.topic";
    var id = "123456";
    var created = scheduled.minus(Duration.ofDays(1));
    var expires = scheduled.plus(Duration.ofDays(1));
    var produced = scheduled.minus(Duration.ofSeconds(5));
    srm = new ScheduledRecordMetadata(scheduled, destination, id, created, expires, produced);
    srmp = Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(scheduled.getEpochSecond()).setNanos(scheduled.getNano()))
        .setDestination(destination)
        .setId(id)
        .setCreated(Timestamp.newBuilder().setSeconds(created.getEpochSecond()).setNanos(created.getNano()))
        .setExpires(Timestamp.newBuilder().setSeconds(expires.getEpochSecond()).setNanos(expires.getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(produced.getEpochSecond()).setNanos(produced.getNano()))
        .build();
    var fromProto = ScheduledRecordMetadataDeserializer.fromProto(srmp);
    assertEquals(srm, fromProto);
  }



}