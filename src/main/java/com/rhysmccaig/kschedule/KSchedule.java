package com.rhysmccaig.kschedule;

import com.typesafe.config.ConfigFactory;

import java.util.Properties;

import com.rhysmccaig.kschedule.util.ConfigUtils;
import com.typesafe.config.Config;


public class KSchedule {
    
    

  public static void main(String[] args) {
    Config conf = ConfigFactory.load();
    Properties producerConfig = ConfigUtils.toProperties(
        conf.getConfig("kafka").withFallback(conf.getConfig("kafka.producer")));
    Properties consumerConfig = ConfigUtils.toProperties(
        conf.getConfig("kafka").withFallback(conf.getConfig("kafka.consumer")));
        
    

  }

}