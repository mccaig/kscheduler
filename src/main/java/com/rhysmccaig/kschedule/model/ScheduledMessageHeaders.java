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
  public static String KSCHEDULE_HEADER_PRODUCED = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "produced");
  public static String KSCHEDULE_HEADER_ERROR = String.join(KSCHEDULE_HEADER_DELIMITER, KSCHEDULE_HEADER_PREFIX, "error");
  private static List<String> headerNames = List.of(
    KSCHEDULE_HEADER_SCHEDULED,
    KSCHEDULE_HEADER_EXPIRES,
    KSCHEDULE_HEADER_TARGET,
    KSCHEDULE_HEADER_PRODUCED,
    KSCHEDULE_HEADER_ERROR
  );

  private Instant scheduled;
  private Instant expires;
  private String target;
  private Instant produced; // When the record was last produced into a topic.
  private String error;



  public ScheduledMessageHeaders(Instant scheduled, Instant expires, String target, Instant produced, String error) {
    this.scheduled = scheduled;
    this.expires = expires;
    this.target = target;
    this.produced = produced;
    this.error = error;
  }
  public ScheduledMessageHeaders(Instant scheduled, Instant expires, String target, Instant produced ) {
    this(scheduled, expires, target, produced, null);
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

  public Instant getProduced() {
    return produced;
  }

  public void setProduced(Instant produced) {
    this.produced = produced;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  // String encoding of instants is nice and readable, but maybe hurts performance too much
  public List<Header> toHeaders() {
    List<Header> headers = List.of();
    if (scheduled != null) {
      headers.add(new RecordHeader(KSCHEDULE_HEADER_SCHEDULED, scheduled.toString().getBytes(StandardCharsets.UTF_8)));
    }
    if (expires != null) {
      headers.add(new RecordHeader(KSCHEDULE_HEADER_EXPIRES, expires.toString().getBytes(StandardCharsets.UTF_8)));
    }
    if (target != null) {
      headers.add(new RecordHeader(KSCHEDULE_HEADER_TARGET, target.getBytes(StandardCharsets.UTF_8)));
    }
    if (produced != null) {
      headers.add(new RecordHeader(KSCHEDULE_HEADER_PRODUCED, produced.toString().getBytes(StandardCharsets.UTF_8)));
    }
    if (error != null) {
      headers.add(new RecordHeader(KSCHEDULE_HEADER_ERROR, error.getBytes(StandardCharsets.UTF_8)));
    }
    return headers;
  }

  public static ScheduledMessageHeaders fromHeaders(Headers headers) {
    var scheduledHeader = headers.lastHeader(KSCHEDULE_HEADER_SCHEDULED);
    var scheduled = (scheduledHeader == null) ? null : Instant.parse(new String(scheduledHeader.value(), StandardCharsets.UTF_8));
    var expiresHeader = headers.lastHeader(KSCHEDULE_HEADER_EXPIRES);
    var expires = (expiresHeader == null) ? null : Instant.parse(new String(expiresHeader.value(), StandardCharsets.UTF_8));
    var targetHeader = headers.lastHeader(KSCHEDULE_HEADER_TARGET);
    var target = (targetHeader == null) ? null : new String(targetHeader.value(), StandardCharsets.UTF_8);
    var producedHeader = headers.lastHeader(KSCHEDULE_HEADER_PRODUCED);
    var produced = (producedHeader == null) ? null : Instant.parse(new String(producedHeader.value(), StandardCharsets.UTF_8));
    var errorHeader = headers.lastHeader(KSCHEDULE_HEADER_ERROR);
    var error = (errorHeader == null) ? null : new String(errorHeader.value(), StandardCharsets.UTF_8);
    return new ScheduledMessageHeaders(scheduled, expires, target, produced, error);
  }

  /**
   * @param headers existing headers object copy headers from
   * @return new headers object with kschedule headers added or updated
   */
  public List<Header> merge(Iterable<Header> headers) {
    List<Header> newHeaders = this.toHeaders();
    for (var header : headers) {
      if (!headerNames.contains(header.key())) { // Copy existing headers that arent kschedule headers
        newHeaders.add(header);
      }
    }
    return newHeaders;
  }

}