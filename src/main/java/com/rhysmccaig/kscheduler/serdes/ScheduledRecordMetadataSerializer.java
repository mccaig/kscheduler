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
    return (metadata == null) ? null : toProto(metadata).toByteArray();
  }

  public static Protos.ScheduledRecordMetadata toProto(ScheduledRecordMetadata metadata) {
    if (metadata == null || metadata.scheduled() == null) {
      return null;
    }
    return Protos.ScheduledRecordMetadata.newBuilder()
        .setScheduled(Timestamp.newBuilder().setSeconds(metadata.scheduled().toEpochMilli()).setNanos(metadata.scheduled().getNano()))
        .setId(metadata.id() == null ? null : metadata.id())
        .setDestination(metadata.destination() == null ? null : metadata.destination())
        .setExpires(metadata.expires() == null ? null :
          Timestamp.newBuilder().setSeconds(metadata.expires().toEpochMilli()).setNanos(metadata.expires().getNano()))
        .setCreated(metadata.created() == null ? null :
          Timestamp.newBuilder().setSeconds(metadata.created().getEpochSecond()).setNanos(metadata.created().getNano()))
        .setProduced(metadata.produced() == null ? null :
          Timestamp.newBuilder().setSeconds(metadata.produced().getEpochSecond()).setNanos(metadata.produced().getNano()))
        .build();
  }

}