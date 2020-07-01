package com.rhysmccaig.kscheduler.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataDeserializer;
import com.rhysmccaig.kscheduler.serialization.ScheduledRecordMetadataSerializer;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class HeaderUtils {
    
  public static final String KSCHEDULER_HEADER_KEY_PREFIX = "KScheduler-";
  public static final String KSCHEDULER_METADATA_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX + "ScheduledRecordMetadata";
  public static final String KSCHEDULER_SCHEDULED_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Scheduled";
  public static final String KSCHEDULER_ID_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "ID";
  public static final String KSCHEDULER_DESTINATION_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Destination";
  public static final String KSCHEDULER_CREATED_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Created";
  public static final String KSCHEDULER_EXPIRES_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Expires";
  public static final String KSCHEDULER_ERROR_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Error";

  private static final ScheduledRecordMetadataDeserializer DESERIALIZER = new ScheduledRecordMetadataDeserializer();
  private static final ScheduledRecordMetadataSerializer SERIALIZER = new ScheduledRecordMetadataSerializer();

  private static final ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(16));

  private HeaderUtils() {
  }

  public static ScheduledRecordMetadata extractMetadata(Headers headers) {
    return extractMetadata(headers, false);
  }

  /**
   * If scheduled record metadata is available in the headers, then extract them.
   * @param headers the headers to extract the metadata from
   * @param remove if true, removes kscheduler specific headers from the headers object before returning
   * @return
   */
  public static ScheduledRecordMetadata extractMetadata(Headers headers, boolean remove) {
    var metadataHeader = headers.lastHeader(KSCHEDULER_METADATA_HEADER_KEY);
    ScheduledRecordMetadata metadata = null;
    if (metadataHeader != null) {
      try {
        metadata = DESERIALIZER.deserialize(null, metadataHeader.value());
      } catch (SerializationException ex) {
        // noop
      }
    }
    // Failed then fallback to individual headers
    if (metadata == null || metadata.scheduled() == null) {
      final var scheduled = parseHeaderAsInstant(headers.lastHeader(KSCHEDULER_SCHEDULED_HEADER_KEY));
      final var expires = Objects.requireNonNullElse(
          parseHeaderAsInstant(headers.lastHeader(KSCHEDULER_EXPIRES_HEADER_KEY)), Instant.MAX);
      final var created = Objects.requireNonNullElse(
        parseHeaderAsInstant(headers.lastHeader(KSCHEDULER_CREATED_HEADER_KEY)), Instant.MIN);
      final var destinationHeader = headers.lastHeader(KSCHEDULER_DESTINATION_HEADER_KEY);
      var id = parseHeaderAsUuid(headers.lastHeader(KSCHEDULER_ID_HEADER_KEY));
      if (scheduled != null && destinationHeader != null && destinationHeader.value() != null) {
        final var destination = new String(destinationHeader.value(), UTF_8);
        if (id == null) {
          // If an ID hasnt been set, then set one now.
          id = UUID.randomUUID();
        }
        metadata = new ScheduledRecordMetadata(scheduled, expires, created, id, destination);
      }
    }
    if (remove) {
      headers.remove(KSCHEDULER_METADATA_HEADER_KEY);
      headers.remove(KSCHEDULER_SCHEDULED_HEADER_KEY);
      headers.remove(KSCHEDULER_ID_HEADER_KEY);
      headers.remove(KSCHEDULER_DESTINATION_HEADER_KEY);
      headers.remove(KSCHEDULER_CREATED_HEADER_KEY);
      headers.remove(KSCHEDULER_EXPIRES_HEADER_KEY);
    }
    return metadata;
  }

  /**
   * Adds a scheduled record metadata header to the headers.
   * @param headers the headers to add the metadata to
   * @param metadata the scheduled record metadata to add to the headers
   * @return
   */
  public static Headers setMetadata(Headers headers, ScheduledRecordMetadata metadata) {
    if (headers == null) {
      headers = new RecordHeaders();
    }
    headers.remove(KSCHEDULER_METADATA_HEADER_KEY);
    if (metadata == null) {
      return headers;
    }
    byte[] bytes;
    try {
      bytes = SERIALIZER.serialize(null, metadata);
    } catch (SerializationException ex) {
      return headers;
    }
    headers.add(KSCHEDULER_METADATA_HEADER_KEY, bytes);
    return headers;
  }

  /**
   * Adds an error header to the headers.
   * @param headers the headers to add the error header to
   * @param error the error message
   * @return
   */
  public static Headers addError(Headers headers, String error) {
    return headers.add(KSCHEDULER_ERROR_HEADER_KEY, error.getBytes(UTF_8));
  }

  /**
   * Attempts to extract an Instant from the header.
   * Returns null if it is unsuccessful in parsing the header to an Instant
   * @param header the header to extract the Instant from
   * @return
   */
  public static Instant parseHeaderAsInstant(Header header) {
    if (header == null || header.value() == null) {
      return null;
    }
    try {
      return Instant.parse(new String(header.value(), UTF_8));
    } catch (DateTimeParseException ex) {
      return null;
    }
  }

  /**
   * Attempts to extract a UUID from the header.
   * Can parse a uuid encoded as bytes, or in standard string format
   * Returns null if it is unsuccessful in parsing the header to an Instant
   * @param header the header to parse
   * @return
   */
  public static UUID parseHeaderAsUuid(Header header) {
    UUID uuid = null;
    if (header != null && header.value() != null 
        && (header.value().length == 16 || header.value().length == 36)) {
      var uuidBytes = header.value();
      if (uuidBytes.length == 16) {
        // Encoded as bytes
        var buffer = TL_BUFFER.get().position(0).put(uuidBytes).flip();
        uuid = SerializationUtils.getUuid(buffer);
      } else {
        // Encoded as a String
        var uuidString = new String(uuidBytes);
        try {
          uuid = UUID.fromString(uuidString);
        } catch (IllegalArgumentException ex) {
          // noop
        }
      }
    }
    return uuid;
  }

}