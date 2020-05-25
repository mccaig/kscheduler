package com.rhysmccaig.kscheduler.model;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ScheduledRecord {
  static final Logger logger = LogManager.getLogger(ScheduledRecord.class); 

  private ScheduledRecordMetadata metadata;
  private byte[] key;
  private byte[] value;
  private List<Header> headers;

  public ScheduledRecord(ConsumerRecord<byte[], byte[]> record) {
    this(record.key(), record.value(), record.headers());
  };

  private ScheduledRecord(byte[] key, byte[] value, Headers headers) {
    this.key = key;
    this.value = value;
    this.metadata = ScheduledRecordMetadata.fromHeaders(headers);
    this.headers = List.of(headers.remove(ScheduledRecordMetadata.HEADER_NAME).toArray());
  }

  public Headers headers() {
    var newHeaders = new RecordHeaders(headers);
    newHeaders.add(metadata.toHeader());
    return newHeaders;
  }

  private List<Header> headerList() {
    var headersList = new ArrayList<>(headers);
    headersList.add(metadata.toHeader());
    return headersList;
  }

  public byte[] key() {
    return key;
  }

  public byte[] value() {
    return value;
  }

  public ScheduledRecordMetadata metadata() {
    return metadata;
  }

  public static ScheduledRecord fromBytes(byte[] bytes) {
    Protos.ScheduledRecord proto;
    try {
      proto = Protos.ScheduledRecord.parseFrom(bytes);
    } catch (InvalidProtocolBufferException ex) {
      proto = null;
    }
    if (proto == null) {
      return null;
    }
    var headersList = proto.getHeadersList().stream()
        .map(h -> new RecordHeader(h.getName(), h.getValue().toByteArray()))
        .toArray(size -> new RecordHeader[size]);
    var headers = (Headers) new RecordHeaders(headersList);
    return new ScheduledRecord(proto.getKey().toByteArray(), proto.getValue().toByteArray(), headers);
  }

  public byte[] toBytes() {
    return Protos.ScheduledRecord.newBuilder()
        .setKey(ByteString.copyFrom(key()))
        .setValue(ByteString.copyFrom(value()))
        .addAllHeaders(headerList().stream()
            .map(h -> Protos.Header.newBuilder()
                .setName(h.key())
                .setValue(ByteString.copyFrom(h.value()))
                .build())
            .collect(Collectors.toList()))
        .build().toByteArray();
  }
  
}