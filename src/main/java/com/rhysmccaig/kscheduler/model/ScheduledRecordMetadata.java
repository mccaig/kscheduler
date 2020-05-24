package com.rhysmccaig.kscheduler.model;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

public final class ScheduledRecordMetadata {
    
  public static String HEADER_NAME = "kscheduler-metadata";

  private UUID uuid;
  private String destination;
  private Instant scheduled;
  private Instant produced;
  private Instant expires;
  private String error;
  
  public ScheduledRecordMetadata(UUID uuid, String destination, Instant scheduled, Instant produced, Instant expires, String error) {
    this.uuid = uuid;
    this.destination = destination;
    this.scheduled = scheduled;
    this.produced = produced;
    this.expires = expires;
    this.error = error;
  }

  public UUID getUUID() {
    return uuid;
  }

  public void setUUID(UUID uuid) {
    this.uuid = uuid;
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
 
  public static ScheduledRecordMetadata fromHeaders(Headers headers) {
    return new ScheduledRecordMetadata(null, null, null, null, null, null);
  }

  public Header toHeader() {
    return new RecordHeader(HEADER_NAME, ByteBuffer.allocate(1));
  }

}

