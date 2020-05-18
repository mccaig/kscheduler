package com.rhysmccaig.kschedule.strategy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;

public class NotAfterStrategy extends SchedulerStrategy{

  public NotAfterStrategy(List<DelayedTopicConfig> delayedTopics, String deadLetterTopic) {
    super(delayedTopics, deadLetterTopic);
  }

  @Override
  public String next(Instant scheduled, Instant expires, String targetTopic) {
    var now = Instant.now();
    if (expires.isBefore(now)) {
      return deadLetterTopic;
    }
    // If things are going smoothly, this wont happen, but if our consumers are lagging it is possible
    if (scheduled.isBefore(now)) {
      return targetTopic;
    }
    // Find the topic delay which is closest to ideal delay (for perfect secheduling) but not after it.
    var idealDelay = Duration.between(now, scheduled);
    var nextTopic = delayedTopicsSet.stream()
        .reduce(null, (acc, dt) -> {
          var topicDelay = dt.getDelay();
          if ((topicDelay.compareTo(idealDelay) <= 0)) {  // topic has a delay under or equal to ideal delay
            if (acc == null || (topicDelay.compareTo(acc.getDelay()) > 0)) { // havent already picked a topic or this topic has a delay closer to ideal
              return dt;
            }
          }
          return acc; // Fall back to returning the existing accumulator
        });
    // if the only topic to match has a negative delay, then this is the closest we can get, sent it to the target topic
    if (nextTopic.getDelay().isNegative()) {
      return targetTopic;
    }
    // return our target topic
    return nextTopic.getTopic();
  }
}