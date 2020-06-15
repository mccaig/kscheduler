package com.rhysmccaig.kscheduler.serialization;

import java.util.Objects;

import com.google.protobuf.ByteString;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.protos.Protos;
import com.rhysmccaig.kscheduler.model.protos.Protos.ScheduledRecordHeader;

import org.apache.kafka.common.serialization.Serializer;

public class ScheduledRecordSerializer implements Serializer<ScheduledRecord> {

  private static ScheduledRecordMetadataSerializer METADATA_SERIALIZER = new ScheduledRecordMetadataSerializer();

  public byte[] serialize(ScheduledRecord data) {
    return serialize(null, data);
  }

  public byte[] serialize(String topic, ScheduledRecord data) {
    return (data == null) ? null : toBytes(data);
  }

  private static byte[] toBytes(ScheduledRecord record) {
    return toProto(record).toByteArray();
  }

  private static Protos.ScheduledRecord toProto(ScheduledRecord record) {
    var builder = Protos.ScheduledRecord.newBuilder()
        .setMetadata(ByteString.copyFrom(METADATA_SERIALIZER.serialize(null, record.metadata())));
    if (Objects.nonNull(record.key()))
      builder.setKey(ByteString.copyFrom(record.key()));
    if (Objects.nonNull(record.value()))
      builder.setValue(ByteString.copyFrom(record.value()));
    var it = record.headers().iterator();
    while (it.hasNext()) {
      var header = it.next();
      builder.addHeaders(
          ScheduledRecordHeader.newBuilder()
              .setKey(header.key())
              .setValue(ByteString.copyFrom(header.value())));
    }
    return builder.build();
  }

}