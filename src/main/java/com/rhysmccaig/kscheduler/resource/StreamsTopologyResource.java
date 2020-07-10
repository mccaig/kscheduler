package com.rhysmccaig.kscheduler.resource;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.streams.Topology;


@Path("/kscheduler/topology")
@Produces(MediaType.TEXT_PLAIN)
public class StreamsTopologyResource {
    
  private Topology topology;

  public StreamsTopologyResource(Topology topology) {
    this.topology = topology;
  }

    @GET
    public String get() {
        return topology.describe().toString();
    }

}