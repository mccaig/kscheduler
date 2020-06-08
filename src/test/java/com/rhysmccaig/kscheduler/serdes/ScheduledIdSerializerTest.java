package com.rhysmccaig.kscheduler.serdes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import com.rhysmccaig.kscheduler.model.ScheduledId;

import org.junit.jupiter.api.Test;

public class ScheduledIdSerializerTest {
  
  private static Instant SCHEDULED = Instant.EPOCH.plus(Duration.ofNanos(123456)).plus(Duration.ofDays(1000));
  private static String ID = "1234";
  private static String DELIMITER = "~";

  private static ScheduledIdSerializer SERIALIZER = new ScheduledIdSerializer();

  @Test
  public void serialize() {
    var expected = new StringBuilder()
      .append(SCHEDULED.getEpochSecond())
      .append(DELIMITER)
      .append(SCHEDULED.getNano())
      .append(DELIMITER)
      .append(ID)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    var record = new ScheduledId(SCHEDULED, ID);
    var actual = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void serialize_no_id() {
    var expected = new StringBuilder()
      .append(SCHEDULED.getEpochSecond())
      .append(DELIMITER)
      .append(SCHEDULED.getNano())
      .append(DELIMITER)
      .toString()
      .getBytes(StandardCharsets.UTF_8);
    var record = new ScheduledId(SCHEDULED, null);
    var actual = SERIALIZER.serialize(null, record);
    assertArrayEquals(expected, actual);
  }

}