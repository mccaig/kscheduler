package com.rhysmccaig.kscheduler.serdes;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.protobuf.Timestamp;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.model.protos.Protos;
import com.rhysmccaig.kscheduler.util.SerializationUtils;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledRecordMetadataSerializer implements Serializer<ScheduledRecordMetadata> {
    
  private static int INSTANT_SIZE = Long.BYTES + Integer.BYTES;
  private static int ID_SIZE = Long.BYTES + Long.BYTES;
  private static final ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INSTANT_SIZE + ID_SIZE));

  public byte[] serialize(String topic, ScheduledRecordMetadata data) {
    if (data == null) {
      return null;
    } else {
      var id = data.id();
      var buffer = TL_BUFFER.get().position(0);
      buffer.put(SerializationUtils.toOrderedBytes(data.scheduled().getEpochSecond()))
            .put(SerializationUtils.toOrderedBytes(data.scheduled().getNano()));
      if (data.id() != null) {
        buffer.putLong(id.getMostSignificantBits())
              .putLong(id.getLeastSignificantBits());
      }
      buffer.flip();
      var bytes = new byte[buffer.limit()];
      buffer.get(bytes, 0, bytes.length);
      return bytes;
    }
  }



  public byte[] serialize(String topic, ScheduledRecordMetadata data) {
    return (data == null) ? null : toBytes(data);
  }

  private static byte[] toBytes(ScheduledRecordMetadata metadata) {
    return (metadata == null) ? null : toProto(metadata).toByteArray();
  }

  protected static Protos.ScheduledRecordMetadata toProto(ScheduledRecordMetadata metadata) {
    if (metadata == null)
      return null;
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