package com.rhysmccaig.kscheduler.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.protos.Protos;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;

public class ScheduledRecordDeserializer implements Deserializer<ScheduledRecord> {
    
  private static ScheduledRecordMetadataDeserializer METADATA_DESERIALIZER = new ScheduledRecordMetadataDeserializer();


  public ScheduledRecord deserialize(byte[] bytes) {
    return deserialize(null, bytes);
  }

  public ScheduledRecord deserialize(String topic, byte[] bytes) {
    return (bytes == null) ? null : fromBytes(bytes);
  }

  private static ScheduledRecord fromBytes(byte[] bytes) {
    Protos.ScheduledRecord proto;
    try {
      proto = Protos.ScheduledRecord.parseFrom(bytes);
    } catch (InvalidProtocolBufferException | NullPointerException ex) {
      throw new SerializationException();
    }
    return fromProto(proto);
  }

  private static ScheduledRecord fromProto(Protos.ScheduledRecord proto) {
    if (proto == null) {
      return null;
    }
    var metadata = METADATA_DESERIALIZER.deserialize(proto.getMetadata().toByteArray());
    var key = proto.getKey().isEmpty() ? null : proto.getKey().toByteArray();
    var value = proto.getValue().isEmpty() ? null : proto.getValue().toByteArray();
    var headers = new RecordHeaders();
    var it = proto.getHeadersList().iterator();
    while (it.hasNext()) {
      var header = it.next();
      headers.add(header.getKey(), header.getValue().toByteArray());
    }
    return new ScheduledRecord(metadata, key, value, headers);
  }

}