package com.rhysmccaig.kscheduler.serdes;

import java.time.Instant;
import java.util.Objects;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledRecordMetadataDeserializer implements Deserializer<ScheduledRecordMetadata> {
    
  public ScheduledRecordMetadata deserialize(String topic, byte[] bytes) {
    return (bytes == null) ? null : fromBytes(bytes);
  }

  private static ScheduledRecordMetadata fromBytes(byte[] bytes) {
    if (Objects.isNull(bytes))
      return null;
    Protos.ScheduledRecordMetadata proto;
    try {
      proto = Protos.ScheduledRecordMetadata.parseFrom(bytes);
    } catch (InvalidProtocolBufferException | NullPointerException ex) {
      throw new SerializationException();
    }
    return fromProto(proto);
  }

  protected static ScheduledRecordMetadata fromProto(Protos.ScheduledRecordMetadata proto) {
    if (Objects.isNull(proto))
      return null;
    if (!proto.hasScheduled() || proto.getDestination().isEmpty()) {
      throw new SerializationException();
    }
    return new ScheduledRecordMetadata(
        Instant.ofEpochSecond(proto.getScheduled().getSeconds(), proto.getScheduled().getNanos()),
        proto.getDestination(),
        proto.getId().isEmpty() ? null : proto.getId(),
        (proto.hasCreated()) ? Instant.ofEpochSecond(proto.getCreated().getSeconds(), proto.getCreated().getNanos()) : null,
        (proto.hasExpires()) ? Instant.ofEpochSecond(proto.getExpires().getSeconds(), proto.getExpires().getNanos()) : null, 
        (proto.hasProduced()) ? Instant.ofEpochSecond(proto.getProduced().getSeconds(), proto.getProduced().getNanos()): null);
  }

}