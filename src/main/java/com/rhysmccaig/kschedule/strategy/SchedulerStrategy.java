package com.rhysmccaig.kschedule.strategy;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;

public abstract class SchedulerStrategy {

  protected final Set<DelayedTopicConfig> delayedTopicsSet;
  protected final String deadLetterTopic;

  public SchedulerStrategy(List<DelayedTopicConfig> delayedTopics, String deadLetterTopic) {
    this.delayedTopicsSet = delayedTopics.stream()
        .sorted((a, b) -> a.getDelay().compareTo(b.getDelay())).collect(Collectors.toSet());
    this.deadLetterTopic = deadLetterTopic;
  }

  public abstract String next(Instant scheduled, Instant expires, String targetTopic);

}