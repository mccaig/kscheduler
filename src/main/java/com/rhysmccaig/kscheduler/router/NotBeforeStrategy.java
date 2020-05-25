package com.rhysmccaig.kscheduler.router;

import java.time.Duration;
import java.time.Instant;
import java.util.SortedSet;

import com.rhysmccaig.kscheduler.model.DelayedTopicConfig;

public class NotBeforeStrategy implements RoutingStrategy {

  public static final Duration DEFAULT_DELAY_GRACE_PERIOD = Duration.ofMinutes(1);
  
  private final Duration delayGracePeriod;
  /**
  * This strategy will attempt to deliver the event to the target as close as possible to the scheduled time,
  * but will never deliver it early. In other words, this strategy:
  * <li> Selects the target topic if the scheduled time has already passed</li>
  * <li> Selects the topic with the highest non negative delay that will result in the event being re-evaluated 
  * at least <code>delayGracePeriod</code> prior to its scheduled delivery time</li>
  * <li> If no topic is available that will result in the event being re-evaluated before the scheduled time, 
  * selects the topic that will result in the message being re-evaluated as close to - but not before, the scheduled time.</li>
  * 
  * * @param delayGracePeriod
  */
  public NotBeforeStrategy(Duration delayGracePeriod) {
    this.delayGracePeriod = delayGracePeriod;
  }

  public NotBeforeStrategy() {
    this(DEFAULT_DELAY_GRACE_PERIOD);
  }
    
  @Override
  public String getNextTopic(SortedSet<DelayedTopicConfig> delayedTopics, Instant currentTime, Instant scheduled, String targetTopic) {
    if (scheduled.isBefore(currentTime)) {
      return targetTopic;
    }
    var idealDelay = Duration.between(currentTime, scheduled.minus(delayGracePeriod));
    var nextTopic = delayedTopics.stream()
        .reduce(null, (next, topic) -> {
          if (!topic.getDelay().isNegative()) { // Only interested in topics with non negative delay
            if (next == null) {                 // If we dont already have a candidate next topic
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
    
}