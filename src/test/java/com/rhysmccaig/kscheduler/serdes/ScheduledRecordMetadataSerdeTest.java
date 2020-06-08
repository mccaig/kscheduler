package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataSerdeTest {
  
  private static Serde<ScheduledRecordMetadata> SERDE = new ScheduledRecordMetadataSerde();
  private static Instant SCHEDULED = Instant.EPOCH;
  private static String DESTINATION =  "destination";
  private static String ID = "1234";
  private static Instant CREATED = Instant.MIN;
  private static Instant EXPIRES = Instant.MAX;
  private static Instant PRODUCED = Instant.EPOCH.minus(Duration.ofSeconds(5));
  private static Deserializer<ScheduledRecordMetadata> deserializer = SERDE.deserializer();
  private static Serializer<ScheduledRecordMetadata> serializer = SERDE.serializer();

  @Test
  public void roundtrip() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, PRODUCED);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_id() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, null, CREATED, EXPIRES, PRODUCED);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_created() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, null, EXPIRES, PRODUCED);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_expires() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, null, PRODUCED);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

  @Test
  public void roundtrip_no_produced() {
    var expected = new ScheduledRecordMetadata(SCHEDULED, DESTINATION, ID, CREATED, EXPIRES, null);
    var actual = deserializer.deserialize(null, serializer.serialize(null, expected));
    assertEquals(expected, actual);
  }

}