package com.rhysmccaig.kscheduler.router;

import java.time.Instant;
import java.util.SortedSet;

import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

public abstract class RoutingStrategy {

  /**
   * 
   * @param delayedTopics Must include at least one topic with a non negative delay. Behaviour is not defined if this is not the case.
   * @param metadata
   * @param now         What time is it now.
   * @return            The next topic to send the message to
   */
  public abstract String nextTopic(SortedSet<DelayedTopicConfig> delayedTopics, ScheduledRecordMetadata metadata, Instant now);

  public String nextTopic(SortedSet<DelayedTopicConfig> delayedTopics, ScheduledRecordMetadata metadata) {
    return nextTopic(delayedTopics, metadata, Instant.now());
  }

}





