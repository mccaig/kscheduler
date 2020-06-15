package com.rhysmccaig.kscheduler.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataDeserializer;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataSerializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class HeaderUtils {
    
    public final static String KSCHEDULER_HEADER_KEY_PREFIX = "KScheduler-";
    public final static String KSCHEDULER_METADATA_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX + "ScheduledRecordMetadata";
    public final static String KSCHEDULER_SCHEDULED_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Scheduled";
    public final static String KSCHEDULER_ID_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "ID";
    public final static String KSCHEDULER_DESTINATION_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Destination";
    public final static String KSCHEDULER_CREATED_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Created";
    public final static String KSCHEDULER_EXPIRES_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Expires";
    public final static String KSCHEDULER_ERROR_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Error";

    private final static ScheduledRecordMetadataDeserializer DESERIALIZER = new ScheduledRecordMetadataDeserializer();
    private final static ScheduledRecordMetadataSerializer SERIALIZER = new ScheduledRecordMetadataSerializer();

    private static final ThreadLocal<ByteBuffer> TL_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(16));

    public static ScheduledRecordMetadata extractMetadata(Headers headers) {
        return extractMetadata(headers, false);
    }

    public static ScheduledRecordMetadata extractMetadata(Headers headers, Boolean remove) {
        var metadataHeader = headers.lastHeader(KSCHEDULER_METADATA_HEADER_KEY);
        ScheduledRecordMetadata metadata = null;
        if (metadataHeader != null) {
            try {
                metadata = DESERIALIZER.deserialize(null, metadataHeader.value());
            } catch (SerializationException ex) {}
        }
        // Failed then fallback to individual headers
        if (metadata == null || metadata.scheduled() == null ) {
            final var scheduled = parseHeaderAsInstant(headers.lastHeader(KSCHEDULER_SCHEDULED_HEADER_KEY));
            final var expires = parseHeaderAsInstant(headers.lastHeader(KSCHEDULER_EXPIRES_HEADER_KEY));
            final var created = parseHeaderAsInstant(headers.lastHeader(KSCHEDULER_CREATED_HEADER_KEY));
            final var destinationHeader = headers.lastHeader(KSCHEDULER_DESTINATION_HEADER_KEY);
            var id = parseHeaderAsUUID(headers.lastHeader(KSCHEDULER_ID_HEADER_KEY));
            if (scheduled != null && expires != null && created != null 
                && destinationHeader != null && destinationHeader.value() != null) {
                if (id == null) {
                    // If an ID hasnt been set, then set one now.
                    id = UUID.randomUUID();
                }
                final var destination = new String(destinationHeader.value(), StandardCharsets.UTF_8);
                metadata = new ScheduledRecordMetadata(scheduled, expires, created, id, destination);
            }
        }
        if (Objects.nonNull(remove) && remove) {
            headers.remove(KSCHEDULER_METADATA_HEADER_KEY);
            headers.remove(KSCHEDULER_SCHEDULED_HEADER_KEY);
            headers.remove(KSCHEDULER_ID_HEADER_KEY);
            headers.remove(KSCHEDULER_DESTINATION_HEADER_KEY);
            headers.remove(KSCHEDULER_CREATED_HEADER_KEY);
            headers.remove(KSCHEDULER_EXPIRES_HEADER_KEY);
        }
        return metadata;
    }

    public static Headers setMetadata(Headers headers, ScheduledRecordMetadata metadata) {
        if (Objects.isNull(headers)) {
            headers = new RecordHeaders();
        }
        headers.remove(KSCHEDULER_METADATA_HEADER_KEY);
        if (Objects.isNull(metadata)) {
            return headers;
        }
        byte[] bytes;
        try {
            bytes = SERIALIZER.serialize(null, metadata);
        } catch (SerializationException ex) {
            return headers;
        }
        if (Objects.isNull(headers)) {
            headers = new RecordHeaders();
        }
        headers.add(KSCHEDULER_METADATA_HEADER_KEY, bytes);
        return headers;
    }

    public static Headers addError(Headers headers, String error) {
        return headers.add(KSCHEDULER_ERROR_HEADER_KEY, error.getBytes(StandardCharsets.UTF_8));
    }

    public static Instant parseHeaderAsInstant(Header header) {
        if (header == null || header.value() == null) {
            return null;
        }
        return Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
    }

    public static UUID parseHeaderAsUUID(Header header) {
        UUID uuid = null;
        if (header != null && header.value() != null 
            && (header.value().length == 16 || header.value().length == 36)) {
            var uuidBytes = header.value();
            
            if (uuidBytes.length == 16) {
                // Encoded as bytes
                var buffer = TL_BUFFER.get().position(0).put(uuidBytes).flip();
                uuid = SerializationUtils.getUUID(buffer);
            } else {
                // Encoded as a String
                var uuidString = new String(uuidBytes);
                try {
                    uuid = UUID.fromString(uuidString);
                } catch (IllegalArgumentException ex) {}
            }
        }
        return uuid;
    }


}