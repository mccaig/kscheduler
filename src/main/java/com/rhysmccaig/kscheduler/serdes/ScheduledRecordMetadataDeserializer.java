package com.rhysmccaig.kscheduler.serdes;

import java.time.Instant;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledRecordMetadataDeserializer implements Deserializer<ScheduledRecordMetadata> {
    
  public ScheduledRecordMetadata deserialize(String topic, byte[] bytes) {
    return (bytes == null) ? null : fromBytes(bytes);
  }

  public static ScheduledRecordMetadata fromBytes(byte[] bytes) {
    Protos.ScheduledRecordMetadata proto;
    try {
      proto = Protos.ScheduledRecordMetadata.parseFrom(bytes);
    } catch (InvalidProtocolBufferException ex) {
      throw new SerializationException();
    }
    return fromProto(proto);
  }

  public static ScheduledRecordMetadata fromProto(Protos.ScheduledRecordMetadata proto) {
    return (proto == null || proto.getScheduled() == null) ? null : new ScheduledRecordMetadata(
      Instant.ofEpochSecond(proto.getScheduled().getSeconds(), proto.getScheduled().getNanos()),
      proto.getDestination(),
      proto.getId(),
      (proto.getCreated() == null) ? null : Instant.ofEpochSecond(proto.getCreated().getSeconds(), proto.getCreated().getNanos()),
      (proto.getExpires() == null) ? null : Instant.ofEpochSecond(proto.getExpires().getSeconds(), proto.getExpires().getNanos()), 
      (proto.getProduced() == null) ? null : Instant.ofEpochSecond(proto.getProduced().getSeconds(), proto.getProduced().getNanos()));
  }

}