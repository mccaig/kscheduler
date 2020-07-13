package com.rhysmccaig.kscheduler;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AppLifecycle {

    private static final Logger LOGGER = Logger.getLogger("AppLifecycle");

    void onStart(@Observes StartupEvent ev) {               
        LOGGER.info("KScheduler starting...");
    }

    void onStop(@Observes ShutdownEvent ev) {               
        LOGGER.info("KScheduler stopping...");
    }

}