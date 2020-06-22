package com.rhysmccaig.kscheduler.util;

import java.util.Properties;
import com.typesafe.config.Config;


// Convert lightbend config into java properties
public class ConfigUtils {

    private ConfigUtils() {
        // noop
    }
    
    public static Properties toProperties(Config config) {
        Properties properties = new Properties();
        config.entrySet().forEach(e -> properties.setProperty(e.getKey(), config.getString(e.getKey())));
        return properties;
    }
}