package com.rhysmccaig.kschedule.model;

import java.time.Instant;

public class ScheduledMessage {
    
    public Instant created;
    public Instant enteredTopic;
    public Instant scheduled;
    public Instant expires;
    public Integer hops;

}