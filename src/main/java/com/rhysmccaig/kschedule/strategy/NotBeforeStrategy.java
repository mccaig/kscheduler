package com.rhysmccaig.kschedule.strategy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.rhysmccaig.kschedule.model.DelayedTopicConfig;

public class NotBeforeStrategy extends SchedulerStrategy{

  private final DelayedTopicConfig shortestNonNegative;

  public NotBeforeStrategy(List<DelayedTopicConfig> delayedTopics, String deadLetterTopic) {
    super(delayedTopics, deadLetterTopic);
    var shortest = delayedTopicsSet.stream()
        .filter(dt -> !dt.getDelay().isNegative())
        .findFirst();
    if (shortest.isEmpty()) {
      throw new Error("NotBeforeStrategy needs at least one delayed topic");
    }
    shortestNonNegative = shortest.get();
  }

  @Override
  public String next(Instant scheduled, Instant expires, String targetTopic) {
    var now = Instant.now();
    if (expires.isBefore(now)) {
      return deadLetterTopic;
    }
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
    // if the only topic to match has a negative delay, then we fall back to the smallest positive delay
    if (nextTopic.getDelay().isNegative()) {
      nextTopic = shortestNonNegative;
    }
    // return our target topic
    return nextTopic.getTopic();
  }
}