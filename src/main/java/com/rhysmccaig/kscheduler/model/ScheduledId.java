package com.rhysmccaig.kscheduler.model;

import java.time.Instant;

public class ScheduledId {

  private static final String DELIMITER = "~";

  private Instant scheduled;
  private String id;

  public ScheduledId(Instant scheduled, String id) {
    if (scheduled == null) {
      throw new NullPointerException("scheduled must not be null");
    }
    this.scheduled = scheduled;
    this.id = id;
  }

  public Instant scheduled() {
    return scheduled;
  }

  public String id() {
    return id;
  }

  @Override
  public String toString() {
    var seconds = scheduled.getEpochSecond();
    var nanos = scheduled.getNano();
    return new StringBuilder()
        .append(seconds)
        .append(DELIMITER)
        .append(nanos)
        .append(DELIMITER)
        .append(id)
        .toString();
  }

  public static ScheduledId fromString(String str) {
    var tokens = str.split(DELIMITER);
    var seconds = Long.parseLong(tokens[0]);
    var nanos = Integer.parseInt(tokens[1]);
    var id = tokens[3];
    var scheduled = Instant.ofEpochSecond(seconds, nanos);
    return new ScheduledId(scheduled, id);
  }
}