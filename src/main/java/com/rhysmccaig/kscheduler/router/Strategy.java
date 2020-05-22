package com.rhysmccaig.kscheduler.router;

import java.time.Duration;
import java.time.Instant;
import java.util.SortedSet;

import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;

/**
 *  Topic Routing strategies that can be used
 *  <li>{@link #NOT_BEFORE}</li>
 *  <li>{@link #NOT_AFTER}</li>
 */
public enum Strategy implements RoutingStrategy {
    /**
    * NOT_BEFORE Strategy
    * <br><br>
    * This strategy will attempt to deliver the event to the target as close as possible to the scheduled time,
    * but will never deliver it early. In other words, this strategy:
    * <li> Selects the target topic if the scheduled time has already passed</li>
    * <li> Selects the topic with the highest non negative delay that will result in the event being re-evaluated 
    * prior to the schedule time</li>
    * <li> If no topic is available that will result in the event being re-evaluated before the scheduled time, 
    * selects the topic that will result in the message being re-evaluated as close to - but after, the schedule time.</li>
    */
    NOT_BEFORE() {
      @Override
      public String getNextTopic(SortedSet<DelayedTopicConfig> delayedTopics, Instant currentTime, Instant scheduled, String targetTopic) {
        if (scheduled.isBefore(currentTime)) {
          return targetTopic;
        }
        var idealDelay = Duration.between(currentTime, scheduled);
        var nextTopic = delayedTopics.stream()
            .reduce(null, (next, topic) -> {
              if (!topic.getDelay().isNegative()) { // Only interested in topics with non negative delay
                if (next == null) {                 // If wedont already have a candidate next topic
                  next = topic;                     // then any topic is a better candidate next topic
                } else if ((next.getDelay().compareTo(idealDelay) <= 0)       // IF the current candidate next topic has a delay lower than the ideal delay
                    && (next.getDelay().compareTo(idealDelay) <= 0)           // AND this topic also has a delay lower than the ideal delay
                    && (next.getDelay().compareTo(topic.getDelay()) <= 0) ) { // AND this topic has a higher delay than the current candidate next topic
                      next = topic;                 // then this is a better candidate next topic
                } else if((next.getDelay().compareTo(idealDelay) > 0)         // IF the current candidate next topic has a delay higher than the ideal delay
                    && (next.getDelay().compareTo(topic.getDelay()) > 0)) {   // AND this topic has a delay lower than the current candidate next topic
                      next = topic;                 // then this is a better candidate next topic
                }
              }
              return next;
            });
        return nextTopic.getTopic();
      }
    },
    /**
    * NOT_AFTER Strategy
    * <br><br>
    * This strategy will attempt to deliver the event to the target as close as possible to the scheduled time,
    * but will never deliver it late in the absence of adverse operation conditions. In other words, this strategy:
    * <li> Selects the target topic if the scheduled time has already passed (likely as a result of consumer lag)</li>
    * <li> Selects the topic with the highest non negative delay that will result in the event being re-evaluated 
    * prior to the schedule time</li>
    * <li> If no topic is available that will result in the event being re-evaluated before the scheduled time, 
    * selects the targetTopic for delivery.</li>
    */
    NOT_AFTER() {
      @Override
      public String getNextTopic(SortedSet<DelayedTopicConfig> delayedTopics, Instant currentTime, Instant scheduled, String targetTopic) {
        if (scheduled.isBefore(currentTime)) { // This will only happen if we are evaluating an event late
          return targetTopic;
        }
        var idealDelay = Duration.between(currentTime, scheduled);
        var nextTopic = delayedTopics.stream()
            .reduce(null, (next, topic) -> {
              if (!topic.getDelay().isNegative()) { // Only interested in topics with non negative delay
                if ((topic.getDelay().compareTo(idealDelay) <= 0)) {  // If topic has a delay under or equal to ideal delay
                  if (next == null || (topic.getDelay().compareTo(next.getDelay()) > 0)) { // havent already picked a topic or this topic has a delay higher than the current next topic
                    return topic;
                  }
                }
              }
              return next;
            });
        return (nextTopic == null) ? targetTopic : nextTopic.getTopic(); // If there are no valid next topics, we're going to the target topic
      }
    };
  }