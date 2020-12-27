package com.rhysmccaig.kscheduler.util;

import com.typesafe.config.Config;
import java.util.Properties;

// Convert lightbend config into java properties
public class ConfigUtils {
  
  /**
   * Convert config object into java properties.
   * @param config typesefe config object
   * @return
   */
  public static Properties toProperties(Config config) {
    Properties properties = new Properties();
    config.entrySet().forEach(e -> properties.setProperty(e.getKey(), config.getString(e.getKey())));
    return properties;
  }

}