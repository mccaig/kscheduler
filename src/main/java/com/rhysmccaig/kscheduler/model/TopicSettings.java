package com.rhysmccaig.kscheduler.model;

import java.util.Objects;

public class TopicSettings {
  
  private final boolean schedulingEnabled;
  private final boolean ignoreTopicErrors;

  public TopicSettings(Boolean schedulingEnabled, Boolean ignoreTopicErrors) {
    this.schedulingEnabled = Objects.requireNonNullElse(schedulingEnabled, true);
    this.ignoreTopicErrors = Objects.requireNonNullElse(ignoreTopicErrors, false);
  }

  public TopicSettings() {
    this(null, null);
  }

  public boolean getSchedulingEnabled() {
    return schedulingEnabled;
  }

  public boolean getIgnoreTopicErrors() {
    return ignoreTopicErrors;
  }

  
}