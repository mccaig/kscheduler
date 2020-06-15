package com.rhysmccaig.kscheduler.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledRecordMetadataSerializerTest {

  private static Instant SCHEDULED = Instant.EPOCH;
  private static Instant EXPIRES = Instant.MAX;
  private static Instant CREATED = Instant.MIN;
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");
  private static String DESTINATION = "topic";
  private static ScheduledRecordMetadataSerializer SERIALIZER = new ScheduledRecordMetadataSerializer();

  @BeforeEach
  public void beforeEach() {

  }

  @Test
  public void serialize() {
    var metadata = new ScheduledRecordMetadata(SCHEDULED, EXPIRES, CREATED, ID, DESTINATION);
    var bytes = SERIALIZER.serialize(null, metadata);
    var expected = ScheduledRecordMetadataSerializer.MINIMUM_SERIALIZED_SIZE + DESTINATION.length();
    assertEquals(expected, bytes.length);
  }

  @Test
  public void serialize_null_returns_null() {
    var bytes = SERIALIZER.serialize(null, null);
    assertArrayEquals(null, bytes);
  }



}