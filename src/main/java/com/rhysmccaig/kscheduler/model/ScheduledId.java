package com.rhysmccaig.kscheduler.model;

import java.time.Instant;

public class ScheduledId {


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




}