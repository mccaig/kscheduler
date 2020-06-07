package com.rhysmccaig.kscheduler.util;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataDeserializer;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataSerializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class HeaderUtils {
    
    public final static String KSCHEDULER_HEADER_KEY_PREFIX = "KScheduler-";
    public final static String KSCHEDULER_METADATA_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX + "ScheduledRecordMetadata";
    public final static String KSCHEDULER_SCHEDULED_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Scheduled";
    public final static String KSCHEDULER_ID_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "ID";
    public final static String KSCHEDULER_DESTINATION_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Destination";
    public final static String KSCHEDULER_CREATED_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Created";
    public final static String KSCHEDULER_EXPIRES_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Expires";
    public final static String KSCHEDULER_PRODUCED_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Produced";
    public final static String KSCHEDULER_ERROR_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Error";

    private final static ScheduledRecordMetadataDeserializer DESERIALIZER = new ScheduledRecordMetadataDeserializer();

    
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
            final var scheduled = tryParseHeaderAsInstant(headers.lastHeader(KSCHEDULER_SCHEDULED_HEADER_KEY));
            final var destinationHeader = headers.lastHeader(KSCHEDULER_DESTINATION_HEADER_KEY);
            final var destination = Objects.isNull(destinationHeader) ? null : new String(destinationHeader.value(), StandardCharsets.UTF_8);
            if (Objects.nonNull(scheduled) && Objects.nonNull(destination)) {
                final var idHeader = headers.lastHeader(KSCHEDULER_ID_HEADER_KEY);
                final String id = Objects.isNull(idHeader) ? UUID.randomUUID().toString() : new String(idHeader.value(), StandardCharsets.UTF_8);
                final var created = tryParseHeaderAsInstant(headers.lastHeader(KSCHEDULER_CREATED_HEADER_KEY));
                final var expires = tryParseHeaderAsInstant(headers.lastHeader(KSCHEDULER_EXPIRES_HEADER_KEY));
                final var produced = tryParseHeaderAsInstant(headers.lastHeader(KSCHEDULER_PRODUCED_HEADER_KEY));
                metadata = new ScheduledRecordMetadata(scheduled, id, destination, created, expires, produced);
            }
        }
        if (Objects.nonNull(remove) && remove) {
            headers.remove(KSCHEDULER_METADATA_HEADER_KEY);
            headers.remove(KSCHEDULER_SCHEDULED_HEADER_KEY);
            headers.remove(KSCHEDULER_ID_HEADER_KEY);
            headers.remove(KSCHEDULER_DESTINATION_HEADER_KEY);
            headers.remove(KSCHEDULER_CREATED_HEADER_KEY);
            headers.remove(KSCHEDULER_EXPIRES_HEADER_KEY);
            headers.remove(KSCHEDULER_PRODUCED_HEADER_KEY);
        }
        return metadata;
    }

    public static Headers setMetadata(Headers headers, ScheduledRecordMetadata metadata) {
        if (metadata == null) {
            return headers;
        }
        byte[] bytes;
        try {
            bytes = ScheduledRecordMetadataSerializer.toBytes(metadata);
        } catch (SerializationException ex) {
            return headers;
        }
        headers.remove(KSCHEDULER_METADATA_HEADER_KEY);
        headers.add(KSCHEDULER_METADATA_HEADER_KEY, bytes);
        return headers;
    }

    public static Headers addError(Headers headers, String error) {
        return headers.add(KSCHEDULER_ERROR_HEADER_KEY, error.getBytes(StandardCharsets.UTF_8));
    }

    public static Instant tryParseHeaderAsInstant(Header header) {
        Instant instant = null;
        if (Objects.nonNull(header)) {
            try {
                instant = Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
            } catch (NumberFormatException ex) {}
        }
        return instant;
    }


}