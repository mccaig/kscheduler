package com.rhysmccaig.kscheduler.serdes;

import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledRecordMetadataSerializer implements Serializer<ScheduledRecordMetadata> {
    
  public byte[] serialize(String topic, ScheduledRecordMetadata data) {
    return (data == null) ? null : toBytes(data);
  }

  public static byte[] toBytes(ScheduledRecordMetadata metadata) {
    return toProto(metadata).toByteArray();
  }

  public static Protos.ScheduledRecordMetadata toProto(ScheduledRecordMetadata metadata) {
    return Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(metadata.scheduled().toEpochMilli()).setNanos(metadata.scheduled().getNano()))
        .setId(metadata.id())
        .setDestination(metadata.destination())
        .setExpires(Timestamp.newBuilder().setSeconds(metadata.expires().toEpochMilli()).setNanos(metadata.expires().getNano()))
        .setCreated(Timestamp.newBuilder().setSeconds(metadata.created().getEpochSecond()).setNanos(metadata.created().getNano()))
        .setProduced(Timestamp.newBuilder().setSeconds(metadata.produced().getEpochSecond()).setNanos(metadata.produced().getNano()))
        .setError(metadata.error())
        .build();
  }

}