package com.rhysmccaig.kscheduler.model;

import java.time.Duration;
import java.util.Objects;

public class DelayedTopicConfig {
    
    private String name;
    private String topic;
    private Duration delay;

    public DelayedTopicConfig(String name, String topic, Duration delay) {
        this.name = name;
        this.topic = topic;
        this.delay = delay;
    }
    
    public String getName() {
        return name;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public Duration getDelay() {
        return delay;
    }

    public String toString() {
        return String.join(",", 
            String.join("=", "name", name), 
            String.join("=", "topic", topic), 
            String.join("=", "delay", String.format("%s", delay.toSeconds())));
    }


  @Override
  public int hashCode() {
    return Objects.hash(name, topic, delay);
  }

@Override
public boolean equals(Object o) {
  if (o == this) return true;
  if (!(o instanceof DelayedTopicConfig)) {
      return false;
  }
  var co = (DelayedTopicConfig) o;
  return Objects.equals(name, co.name)
      && Objects.equals(topic, co.topic)
      && Objects.equals(delay, co.delay);
}

}