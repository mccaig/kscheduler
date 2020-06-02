package com.rhysmccaig.kscheduler.serdes;

import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledRecordDeserializer implements Deserializer<ScheduledRecord> {
    
  public ScheduledRecord deserialize(String topic, byte[] bytes) {
    return (bytes == null) ? null : fromBytes(bytes);
  }

  private static ScheduledRecord fromBytes(byte[] bytes) {
    Protos.ScheduledRecord proto;
    try {
      proto = Protos.ScheduledRecord.parseFrom(bytes);
    } catch (InvalidProtocolBufferException ex) {
      throw new SerializationException();
    }
    return fromProto(proto);
  }

  public static ScheduledRecord fromProto(Protos.ScheduledRecord proto) {
    if (proto == null) {
      return null;
    }
    var metadata = ScheduledRecordMetadataDeserializer.fromProto(proto.getMetadata());
    var key = proto.getKey().toByteArray();
    var value = proto.getValue().toByteArray();
    var headersList = proto.getHeadersList().stream()
        .map(rh -> (Header) new RecordHeader(rh.getKey(), rh.getValue().toByteArray()))
        .collect(Collectors.toList());
    return new ScheduledRecord(metadata, key, value, new RecordHeaders(headersList));
  }

}