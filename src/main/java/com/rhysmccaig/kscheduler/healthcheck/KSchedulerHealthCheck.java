package com.rhysmccaig.kscheduler.healthcheck;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

@Liveness
@ApplicationScoped
public class KSchedulerHealthCheck implements HealthCheck {

  @Inject
  KafkaStreams streams; 

  @Override
  public HealthCheckResponse call() {
    System.out.println(streams);
    return HealthCheckResponse.named("KScheduler Core")
        .state(streams.state().isRunningOrRebalancing())
        .withData("streams", streams.state().toString())
        .build();
  }
}