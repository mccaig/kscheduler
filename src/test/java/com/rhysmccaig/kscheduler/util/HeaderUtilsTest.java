package com.rhysmccaig.kscheduler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class HeaderUtilsTest {
  
  public static String EMPTY_STRING = new String();


@Test
public void parseEpoch() {
  var expected = Instant.EPOCH;
  var instantString = "1970-01-01T00:00:00Z";
  RecordHeader header = new RecordHeader(EMPTY_STRING, instantString.getBytes(StandardCharsets.UTF_8));
  var actual = HeaderUtils.tryParseHeaderAsInstant(header);
  assertEquals(expected, actual);
}

@Test
public void parseEpochWithNanos() {
  var expected = Instant.EPOCH.plus(Duration.ofNanos(123456789L));
  var instantString = "1970-01-01T00:00:00.123456789Z";
  RecordHeader header = new RecordHeader(EMPTY_STRING, instantString.getBytes(StandardCharsets.UTF_8));
  var actual = HeaderUtils.tryParseHeaderAsInstant(header);
  assertEquals(expected, actual);
}

@ParameterizedTest
@CsvSource({
  "1970-01-01T01:00:00+01:00",    // Zulu TZ only
  "1970-01-01T01:00:00",          // no TZ
  "1970-01-01T01:00:00.123456789", // no TZ
  "xyzzy", // gibberish
  "1970-01-01T01:00:00.1234567890Z" // too many decimals
})
public void parseNullOnInvalidTimezoneString(String instantString) {
  var key = EMPTY_STRING;
  byte[] value = instantString.getBytes(StandardCharsets.UTF_8);
  RecordHeader header = new RecordHeader(key, value);
  var actual = HeaderUtils.tryParseHeaderAsInstant(header);
  assertEquals(null, actual);
}

@Test
public void parseNullOnEmptyTimezoneString() {
  var key = EMPTY_STRING;
  byte[] value = "".getBytes(StandardCharsets.UTF_8);
  RecordHeader header = new RecordHeader(key, value);
  var actual = HeaderUtils.tryParseHeaderAsInstant(header);
  assertEquals(null, actual);
}

@Test
public void setMetadata(){
  assertTrue(false, "Not Implemented");
}

@Test
public void extractMetadata() {
  assertTrue(false, "Not Implemented");
}


}