package com.rhysmccaig.kscheduler.resource;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.rhysmccaig.kscheduler.model.ScheduledRecord;
import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

@Path("/scheduled")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ScheduledResource {
    
  public ScheduledResource() {}

    @GET
    public Set<ScheduledRecordMetadata> list() {
        return null;
    }

    @POST
    public ScheduledRecordMetadata add(ScheduledRecord record) {
      return null;
    }

}