package com.rhysmccaig.kscheduler.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class ScheduledRecordMetadata {

  private Instant scheduled;
  private Instant expires;
  private Instant created;
  private UUID id;
  private String destination;
  
  public ScheduledRecordMetadata(Instant scheduled, Instant expires, Instant created, UUID id, String destination) {
    Objects.requireNonNull(scheduled, "scheduled must not be null");
    Objects.requireNonNull(destination, "destination must not be null");
    this.scheduled = scheduled;
    this.expires = expires;
    this.created = created;
    this.id = id;
    this.destination = destination;
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

  public UUID id() {
    return id;
  }

  public String destination() {
    return destination;
  }

  @Override
  public int hashCode() {
      return Objects.hash(scheduled, expires, created, id, destination);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ScheduledRecordMetadata)) {
        return false;
    }
    var co = (ScheduledRecordMetadata) o;
    return Objects.equals(scheduled, co.scheduled)
        && Objects.equals(expires, co.expires)
        && Objects.equals(created, co.created)
        && Objects.equals(id, co.id) 
        && Objects.equals(destination, co.destination);
  }

  @Override
  public String toString() {
    return new StringBuilder().append(ScheduledRecordMetadata.class.getSimpleName())
      .append("{scheduled=")
      .append(scheduled)
      .append(", expires=")
      .append(expires)
      .append(", created=")
      .append(created)
      .append(", id=")
      .append(id)
      .append(", destination=")
      .append(destination)
      .append("}")
      .toString();
  }

}

