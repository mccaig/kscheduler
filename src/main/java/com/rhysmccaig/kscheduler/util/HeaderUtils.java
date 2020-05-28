package com.rhysmccaig.kscheduler.util;

import java.nio.charset.StandardCharsets;

import com.rhysmccaig.kscheduler.model.ScheduledRecordMetadata;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataDeserializer;
import com.rhysmccaig.kscheduler.serdes.ScheduledRecordMetadataSerializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

public class HeaderUtils {
    
    public final static String KSCHEDULER_HEADER_KEY_PREFIX = "KScheduler-";
    public final static String KSCHEDULER_METADATA_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX + "ScheduledRecordMetadata";
    public final static String KSCHEDULER_ERROR_HEADER_KEY = KSCHEDULER_HEADER_KEY_PREFIX +  "Error";

    public static ScheduledRecordMetadata extractMetadata(Headers headers) {
        var header = headers.lastHeader(KSCHEDULER_METADATA_HEADER_KEY);
        if (header == null) {
          return null;
        }
        ScheduledRecordMetadata metadata = null;
        try {
            metadata = ScheduledRecordMetadataDeserializer.fromBytes(header.value());
        } catch (SerializationException ex) {}
        return metadata;
        // TODO: Add fallback code to check for individual headers;
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


}