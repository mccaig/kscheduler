package com.rhysmccaig.kscheduler.serdes;

import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledRecordSerializer implements Serializer<ScheduledRecord> {

  public byte[] serialize(String topic, ScheduledRecord data) {
    return (data == null) ? null : toBytes(data);
  }

  private static byte[] toBytes(ScheduledRecord record) {
    return toProto(record).toByteArray();
  }

  private static Protos.ScheduledRecord toProto(ScheduledRecord record) {
    List<Protos.ScheduledRecordHeader> headers = (record.headers() == null) ? List.of() : List.of(record.headers().toArray()).stream()
        .map(h -> Protos.ScheduledRecordHeader.newBuilder()
            .setKey(h.key())
            .setValue(ByteString.copyFrom(h.value()))
            .build())
        .collect(Collectors.toList());
    return Protos.ScheduledRecord.newBuilder()
        .setMetadata(ScheduledRecordMetadataSerializer.toProto(record.metadata()))
        .setKey(ByteString.copyFrom(record.key()))
        .setValue(ByteString.copyFrom(record.value()))
        .addAllHeaders(headers)
        .build();
  }

}