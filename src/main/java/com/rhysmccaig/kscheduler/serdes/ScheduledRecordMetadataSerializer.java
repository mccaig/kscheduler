package com.rhysmccaig.kscheduler.serdes;

import java.util.Objects;

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
    if (metadata == null) {
      return null;
    }
    var protoBuilder = Protos.ScheduledRecordMetadata.newBuilder();
    if (Objects.nonNull(metadata.scheduled()))
      protoBuilder.setScheduled(Timestamp.newBuilder().setSeconds(metadata.scheduled().getEpochSecond()).setNanos(metadata.scheduled().getNano()));
    if (Objects.nonNull(metadata.id()))
      protoBuilder.setId(metadata.id());
    if (Objects.nonNull(metadata.destination()))
      protoBuilder.setDestination(metadata.destination());
    if (Objects.nonNull(metadata.expires()))
      protoBuilder.setExpires(Timestamp.newBuilder().setSeconds(metadata.expires().getEpochSecond()).setNanos(metadata.expires().getNano()));
    if (Objects.nonNull(metadata.created()))
      protoBuilder.setCreated(Timestamp.newBuilder().setSeconds(metadata.created().getEpochSecond()).setNanos(metadata.created().getNano()));
    if (Objects.nonNull(metadata.produced()))
      protoBuilder.setProduced(Timestamp.newBuilder().setSeconds(metadata.produced().getEpochSecond()).setNanos(metadata.produced().getNano()));
    return protoBuilder.build();
  }

}