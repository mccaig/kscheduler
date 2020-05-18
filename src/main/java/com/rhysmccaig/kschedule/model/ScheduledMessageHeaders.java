package com.rhysmccaig.kschedule.model;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

public class ScheduledMessageHeaders {

  private static String KSCHEDULE_HEADER_PREFIX = "kschedule";
  private static String KSCHEDULE_HEADER_DELIMITER = "-";
  public static String KSCHEDULE_HEADER_SCHEDULED = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "scheduled");
  public static String KSCHEDULE_HEADER_EXPIRES = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "expires");
  public static String KSCHEDULE_HEADER_TARGET = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "target");
  
  private Instant scheduled;
  private Instant expires;
  private String target;

  public ScheduledMessageHeaders(Instant scheduled, Instant expires, String target) {
    this.scheduled = scheduled;
    this.expires = expires;
    this.target = target;
  }

  public Instant getScheduled() {
    return scheduled;
  }

  public void setScheduled(Instant scheduled) {
    this.scheduled = scheduled;
  }

  public Instant getExpiresd() {
    return expires;
  }

  public void setExpires(Instant expires) {
    this.expires = expires;
  }

  public String getTarget() {
    return target;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public Iterable<Header> toHeaders() {
    List<Header> list = List.of();
    if (scheduled != null) {
      list.add(new RecordHeader(KSCHEDULE_HEADER_SCHEDULED, scheduled.toString().getBytes(StandardCharsets.UTF_8)));
    }
    if (expires != null) {
      list.add(new RecordHeader(KSCHEDULE_HEADER_EXPIRES, expires.toString().getBytes(StandardCharsets.UTF_8)));
    }
    if (target != null) {
      list.add(new RecordHeader(KSCHEDULE_HEADER_TARGET, target.getBytes(StandardCharsets.UTF_8)))
    }
    return list;
  }

  public static ScheduledMessageHeaders fromHeaders(Headers headers) {
    var scheduledHeader = headers.lastHeader(KSCHEDULE_HEADER_SCHEDULED);
    var scheduled = (scheduledHeader == null) ? null : Instant.parse(new String(scheduledHeader.value(), StandardCharsets.UTF_8));
    var expiresHeader = headers.lastHeader(KSCHEDULE_HEADER_EXPIRES);
    var expires = (expiresHeader == null) ? null : Instant.parse(new String(expiresHeader.value(), StandardCharsets.UTF_8));
    var targetHeader = headers.lastHeader(KSCHEDULE_HEADER_TARGET);
    var target = (targetHeader == null) ? null : new String(targetHeader.value(), StandardCharsets.UTF_8);
    return new ScheduledMessageHeaders(scheduled, expires, target);
  }

}