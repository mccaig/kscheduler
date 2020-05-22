package com.rhysmccaig.kscheduler.router;

import java.time.Instant;
import java.util.SortedSet;

import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;

public interface RoutingStrategy {
  /**
   * 
   * @param delayedTopics Must include at least one topic with a non negative delay. Behaviour is not defined if this is not the case.
   * @param currentTime   What time is it now?
   * @param scheduled     At what time should the message be scheduled?
   * @param targetTopic   Where are we ssending the message at the scheduled time.
   * @return              The next topic to send the message to
   */
  public String getNextTopic(SortedSet<DelayedTopicConfig> delayedTopics, Instant currentTime, Instant scheduled, String targetTopic);

}



