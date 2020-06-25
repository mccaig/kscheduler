package com.rhysmccaig.kscheduler.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataDeserializer;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class HeaderUtilsTest {
  
  public static String EMPTY_STRING = new String();
  public static ScheduledRecordMetadataSerializer SERIALIZER = new ScheduledRecordMetadataSerializer();
  public static ScheduledRecordMetadataDeserializer DESERIALIZER = new ScheduledRecordMetadataDeserializer();
  private static UUID ID = UUID.fromString("a613b80d-56c3-474b-9d6c-25d8273aa111");

  private ScheduledRecordMetadata metadata;

  @BeforeEach
  public void before() {
    metadata = new ScheduledRecordMetadata(Instant.EPOCH, Instant.MAX, Instant.MIN, ID, "topic");
  }


  @Test
  public void parseEpoch() {
    var expected = Instant.EPOCH;
    var instantString = "1970-01-01T00:00:00Z";
    RecordHeader header = new RecordHeader(EMPTY_STRING, instantString.getBytes(UTF_8));
    var actual = HeaderUtils.parseHeaderAsInstant(header);
    assertEquals(expected, actual);
  }

  @Test
  public void parseEpochWithNanos() {
    var expected = Instant.EPOCH.plus(Duration.ofNanos(123456789L));
    var instantString = "1970-01-01T00:00:00.123456789Z";
    RecordHeader header = new RecordHeader(EMPTY_STRING, instantString.getBytes(UTF_8));
    var actual = HeaderUtils.parseHeaderAsInstant(header);
    assertEquals(expected, actual);
  }

  /**
   * Tests that invalid Instant strings return null.
   * @param instantString the string to parse
   */
  @ParameterizedTest
  @CsvSource({
      "1970-01-01T01:00:00+01:00",    // no TZ
      "1970-01-01T01:00:00",          // no TZ
      "1970-01-01T01:00:00.123456789", // no TZ
      "xyzzy", // gibberish
      "1970-01-01T01:00:00.1234567890Z" // too many decimals
  })
  public void parseNullOnInvalidTimezoneString(String instantString) {
    var key = EMPTY_STRING;
    byte[] value = instantString.getBytes(UTF_8);
    RecordHeader header = new RecordHeader(key, value);
    var actual = HeaderUtils.parseHeaderAsInstant(header);
    assertEquals(null, actual);
  }

  @Test
  public void parseNullOnEmptyTimezoneString() {
    var key = EMPTY_STRING;
    byte[] value = "".getBytes(UTF_8);
    RecordHeader header = new RecordHeader(key, value);
    var actual = HeaderUtils.parseHeaderAsInstant(header);
    assertEquals(null, actual);
  }



  @Test
  public void setMetadataReturnsExistingHeadersOnNullMetadataAndEmptyHeaders() {
    var expected = new RecordHeaders();
    var original = new RecordHeaders();
    var actual = HeaderUtils.setMetadata(original, null);
    assertEquals(expected, actual);
    // original should be the same object as actual
    assertEquals(original, actual);
  }

  @Test
  public void setMetadataReturnsExistingHeadersOnNullMetadataAndSingleHeader() {
    var key = "Hello";
    var value = "World".getBytes(UTF_8);
    var expected = new RecordHeaders();
    expected.add(key, value);
    var original = new RecordHeaders();
    original.add(key, value);
    var actual = HeaderUtils.setMetadata(original, null);
    assertEquals(expected, actual);
    // original should be the same object as actual
    assertEquals(original, actual);
  }

  @Test
  public void setMetadataRemovesExistingMetadataOnNullMetadata() {
    var key = HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY;
    var value = SERIALIZER.serialize(null, metadata);
    var expected = new RecordHeaders();
    var original = new RecordHeaders();
    original.add(key, value);
    var actual = HeaderUtils.setMetadata(original, null);
    assertEquals(expected, actual);
  }

  @Test
  public void setMetadataRemovesExistingMetadataOnNullMetadataAndOtherHeaders() {
    var otherKey = "Hello";
    var otherValue = "World".getBytes(UTF_8);
    var key = HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY;
    var value = SERIALIZER.serialize(null, metadata);
    var expected = new RecordHeaders();
    expected.add(otherKey, otherValue);
    var original = new RecordHeaders();
    original.add(otherKey, otherValue);
    original.add(key, value);
    var actual = HeaderUtils.setMetadata(original, null);
    assertEquals(expected, actual);
  }

  @Test
  public void setMetadataReplacesExistingMetadataOnMetadataAndOtherHeaders() {
    var otherKey = "Hello";
    var otherValue = "World".getBytes(UTF_8);
    var originalMetadata = metadata;
    var key = HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY;
    var value = SERIALIZER.serialize(null, originalMetadata);
    var newMetadata = metadata;
    var original = new RecordHeaders();
    original.add(otherKey, otherValue);
    original.add(key, value);
    var result = HeaderUtils.setMetadata(original, newMetadata);
    assertEquals(2, result.toArray().length);
    assertArrayEquals(result.lastHeader(otherKey).value(), otherValue);
    var newMetadataBytes = SERIALIZER.serialize(null, newMetadata);
    assertArrayEquals(result.lastHeader(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY).value(), newMetadataBytes); 
  }

  @Test
  public void setMetadataSetsMetadataOnMetadataAndOtherHeaders() {
    var otherKey = "Hello";
    var otherValue = "World".getBytes(UTF_8);
    var newMetadata = metadata;
    var original = new RecordHeaders();
    original.add(otherKey, otherValue);
    var result = HeaderUtils.setMetadata(original, newMetadata);
    assertEquals(2, result.toArray().length);
    assertArrayEquals(result.lastHeader(otherKey).value(), otherValue);
    var newMetadataBytes = SERIALIZER.serialize(null, newMetadata);
    assertArrayEquals(result.lastHeader(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY).value(), newMetadataBytes); 
  }



  @Test
  public void extractMetadata_MetadataHeaderOnly() {
    var headers = new RecordHeaders();
    var serialized = SERIALIZER.serialize(null, metadata);
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, serialized);
    var result = HeaderUtils.extractMetadata(headers);
    assertEquals(metadata, result);
    assertEquals(headers.lastHeader(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY).value(), serialized);
  }

  @Test
  public void extractMetadata_MetadataHeaderOnlyRemovesMetadataHeader() {
    var headers = new RecordHeaders();
    var serialized = SERIALIZER.serialize(null, metadata);
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, serialized);
    var result = HeaderUtils.extractMetadata(headers, true);
    assertEquals(metadata, result);
    assertNull(headers.lastHeader(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY));
  }

  @Test
  public void extractMetadata_MetadataHeaderAndOtherHeaderRemovesMetadataHeader() {
    var otherKey = "Hello";
    var otherValue = "World".getBytes(UTF_8);
    var headers = new RecordHeaders();
    var serialized = SERIALIZER.serialize(null, metadata);
    headers.add(otherKey, otherValue);
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, serialized);
    var expected = new RecordHeader(otherKey, otherValue);
    var result = HeaderUtils.extractMetadata(headers, true);
    assertEquals(metadata, result);
    assertNull(headers.lastHeader(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY));
    assertEquals(expected, headers.lastHeader(otherKey));
  }

  @Test
  public void extractMetadata_NoMetadataHeaderFullKSchedulerHeaders() {
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY, metadata.scheduled().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY, metadata.expires().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY, metadata.created().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_ID_HEADER_KEY, metadata.id().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, metadata.destination().getBytes(UTF_8));
    var expected = metadata;
    var result = HeaderUtils.extractMetadata(headers);
    assertEquals(expected, result);
    assertNotNull(headers.lastHeader(HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY));
    assertNotNull(headers.lastHeader(HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY));
    assertNotNull(headers.lastHeader(HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY));
    assertNotNull(headers.lastHeader(HeaderUtils.KSCHEDULER_ID_HEADER_KEY));
    assertNotNull(headers.lastHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY));
  }

  @Test
  public void extractMetadata_NoMetadataHeaderFullKSchedulerHeadersRemovesMetadataHeaders() {
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY, metadata.scheduled().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY, metadata.expires().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY, metadata.created().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_ID_HEADER_KEY, metadata.id().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, metadata.destination().getBytes(UTF_8));
    var expected = metadata;
    var result = HeaderUtils.extractMetadata(headers, true);
    assertEquals(expected, result);
    assertNull(headers.lastHeader(HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY));
    assertNull(headers.lastHeader(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY));
    assertNull(headers.lastHeader(HeaderUtils.KSCHEDULER_ID_HEADER_KEY));
    assertNull(headers.lastHeader(HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY));
    assertNull(headers.lastHeader(HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY));
  }

  @Test
  public void extractMetadata_MetadataHeaderFullKSchedulerHeadersUseMetadataHeader() {
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, SERIALIZER.serialize(null, metadata));
    headers.add(HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY, metadata.expires().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY, metadata.scheduled().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY, metadata.created().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_ID_HEADER_KEY, metadata.id().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, metadata.destination().getBytes(UTF_8));
    var expected = metadata;
    var result = HeaderUtils.extractMetadata(headers);
    assertEquals(expected, result);
  }

  @Test
  public void extractMetadata_MetadataHeaderFullKscheduleHeadersFallbackOnBAdMetadataHeader() {
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_METADATA_HEADER_KEY, new byte[] {10, 0});
    headers.add(HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY, metadata.scheduled().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY, metadata.expires().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY, metadata.created().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_ID_HEADER_KEY, metadata.id().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_DESTINATION_HEADER_KEY, metadata.destination().getBytes(UTF_8));
    var expected = metadata;
    var result = HeaderUtils.extractMetadata(headers);
    assertEquals(expected, result);
  }

  @Test
  public void extractMetadata_NoMetadataHeaderMissingKSchedulerHeaderReturnsNull() {
    var headers = new RecordHeaders();
    headers.add(HeaderUtils.KSCHEDULER_SCHEDULED_HEADER_KEY, metadata.scheduled().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_EXPIRES_HEADER_KEY, metadata.expires().toString().getBytes(UTF_8));
    headers.add(HeaderUtils.KSCHEDULER_CREATED_HEADER_KEY, metadata.created().toString().getBytes(UTF_8));
    var result = HeaderUtils.extractMetadata(headers);
    assertEquals(null, result);
  }


}