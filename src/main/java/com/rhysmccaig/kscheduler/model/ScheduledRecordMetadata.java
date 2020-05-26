package com.rhysmccaig.kscheduler.model;

import java.time.Instant;

public final class ScheduledRecordMetadata {
    
  private Instant scheduled;
  private String id;
  private String destination;
  private Instant created;
  private Instant expires;
  private Instant produced;
  private String error;
  
  public ScheduledRecordMetadata(Instant scheduled, String id, String destination, Instant created, Instant expires, Instant produced, String error) {
    if (scheduled == null) {
      throw new NullPointerException("scheduled must not be null");
    }
    this.scheduled = scheduled;
    this.id = id;
    this.destination = destination;
    this.created = created;
    this.expires = expires;
    this.produced = produced;
    this.error = error;
  }

  public String id() {
    return id;
  }

  public String destination() {
    return destination;
  }

  public Instant scheduled() {
    return scheduled;
  }

  public Instant expires() {
    return expires;
  }

  public Instant created() {
    return created;
  }

  public Instant produced() {
    return produced;
  }

  public void setProduced(Instant produced) {
    this.produced = produced;
  }

  public String error() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

}

