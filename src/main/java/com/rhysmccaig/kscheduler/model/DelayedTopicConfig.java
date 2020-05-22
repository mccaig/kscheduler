package com.rhysmccaig.kscheduler.model;

import java.time.Duration;

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

}