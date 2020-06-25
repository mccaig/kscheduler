package com.rhysmccaig.kscheduler.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class ScheduledId {


  private Instant scheduled;
  private UUID id;

  public ScheduledId(Instant scheduled, UUID id) {
    this.scheduled = Objects.requireNonNull(scheduled, "scheduled must not be null");
    this.id = id;
  }

  public Instant scheduled() {
    return scheduled;
  }

  public UUID id() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheduled, id);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (!(o instanceof ScheduledId)) {
      return false;
    }
    var co = (ScheduledId) o;
    return Objects.equals(scheduled, co.scheduled)
        && Objects.equals(id, co.id);
  }


}