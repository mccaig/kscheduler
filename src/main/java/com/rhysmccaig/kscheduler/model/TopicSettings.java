package com.rhysmccaig.kscheduler.model;

import java.util.Objects;

public class TopicSettings {
  
  private boolean schedulingDisabled = false;
  private boolean ignoreMissingTopic = false;

  public TopicSettings(Boolean schedulingDisabled, Boolean ignoreMissingTopic) {
    this.schedulingDisabled = Objects.requireNonNullElse(schedulingDisabled, false);
    this.ignoreMissingTopic = Objects.requireNonNullElse(ignoreMissingTopic, false);
  }

  public boolean getSchedulingDisabled() {
    return schedulingDisabled;
  }

  public boolean getIgnoreMissingTopic() {
    return ignoreMissingTopic;
  }
  
}