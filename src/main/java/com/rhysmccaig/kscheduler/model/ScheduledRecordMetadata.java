package com.rhysmccaig.kscheduler.model;

import java.time.Instant;
import java.util.Objects;

public final class ScheduledRecordMetadata {

  private Instant scheduled;
  private String destination;
  private String id;
  private Instant created;
  private Instant expires;
  private Instant produced;
  
  public ScheduledRecordMetadata(Instant scheduled, String destination, String id, Instant created, Instant expires, Instant produced) {
    Objects.requireNonNull(scheduled, "scheduled must not be null");
    Objects.requireNonNull(destination, "destination must not be null");
    this.scheduled = scheduled;
    this.destination = destination;
    this.id = id;
    this.created = created;
    this.expires = expires;
    this.produced = produced;
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

}

