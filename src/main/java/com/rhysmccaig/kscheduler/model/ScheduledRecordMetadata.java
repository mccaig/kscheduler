package com.rhysmccaig.kscheduler.model;

import java.time.Instant;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import com.google.protobuf.InvalidProtocolBufferException;

public final class ScheduledRecordMetadata {
    
  public static String HEADER_NAME = "kscheduler-metadata";


  private String id;
  private String destination;
  private Instant scheduled;
  private Instant produced;
  private Instant expires;
  private String error;
  
  public ScheduledRecordMetadata(String id, String destination, Instant scheduled, Instant produced, Instant expires, String error) {
    this.id = id;
    this.destination = destination;
    this.scheduled = scheduled;
    this.produced = produced;
    this.expires = expires;
    this.error = error;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public Instant getScheduled() {
    return scheduled;
  }

  public void setScheduled(Instant scheduled) {
    this.scheduled = scheduled;
  }

  public Instant getProduced() {
    return produced;
  }

  public void setProduced(Instant produced) {
    this.produced = produced;
  }

  public Instant getExpires() {
    return expires;
  }

  public void setExpires(Instant expires) {
    this.expires = expires;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public byte[] toBytes() {
    return Protos.ScheduledRecordMetadata.newBuilder()
        .setId(id)
        .setDestination(destination)
        .setScheduled(scheduled.toEpochMilli())
        .setProduced(produced.toEpochMilli())
        .setExpires(expires.toEpochMilli())
        .setError(error)
        .build().toByteArray();
  }

  public Header toHeader() {
    return new RecordHeader(HEADER_NAME, this.toBytes());
  }

  public static ScheduledRecordMetadata fromBytes(byte[] bytes) {
    Protos.ScheduledRecordMetadata proto;
    try {
      proto = Protos.ScheduledRecordMetadata.parseFrom(bytes);
    } catch (InvalidProtocolBufferException ex) {
      proto = null;
    }
    if (proto == null) {
      return null;
    }
   return new ScheduledRecordMetadata(
      proto.getId(), 
      proto.getDestination(), 
      Instant.ofEpochMilli(proto.getScheduled()),
      Instant.ofEpochMilli(proto.getProduced()),
      Instant.ofEpochMilli(proto.getExpires()), 
      proto.getError());
  }
  
  public static ScheduledRecordMetadata fromHeaders(Headers headers) {
    return ScheduledRecordMetadata.fromBytes(headers.lastHeader(HEADER_NAME).value());
  }

  

}

